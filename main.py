import os
import json
import time
import subprocess
import urllib.parse
import sage_data_client
import pandas as pd
from cmflib import cmf
from cmflib import cmfquery
import threading
import argparse
import tarfile

class CmfProxy:
    def __init__(self,config_path="config.json"):
        with open(config_path) as f:
            config = json.load(f)
        self.sage_server_config = config["sage_server"]
        self.cmf_server_config = config["cmf_server"]
        self.pipeline_config= config["pipeline"]
        self.parent_dir = os.path.dirname(os.path.abspath(__file__))

        self.data_api_url= self.sage_server_config.get('data_api_url')
        self.nodes = self.sage_server_config.get('nodes')
        self.plugin = self.sage_server_config.get('plugin')
        self.from_time = self.sage_server_config.get('history_period', "-1h")
        self.portal_user = self.sage_server_config.get('portal_username')
        self.portal_token = self.sage_server_config.get('portal_access_token')

        self.query_checkpoint = "queried_files.txt" #keep a list of files to be downloaded
        self.download_dir = "cmf_downloads" #where to keep the downloaded files
        self.download_checkpoint = "downloaded_files.txt"

        self.push_checkpoint = "pushed_files.txt"
        self.pipeline_name=self.pipeline_config.get('pipeline_name')
        self.pipeline_file=self.pipeline_config.get('pipeline_file')
        self.stage_name=self.pipeline_config.get('stage_name')
        self.execution_name = self.pipeline_config.get('execution_name')

        self.cmf_init_command = self.cmf_server_config.get("CMF_INIT_COMMAND")

    def query_from_sage(self,output_txt=None):
        '''query the list of downloadable files from the data API'''
        if output_txt is None:
            output_txt = self.query_checkpoint
        all_tar_gz = []
        for node in self.nodes:
            df = sage_data_client.query(
                start=self.from_time,
                filter={
                    "plugin": self.plugin,
                    "vsn": node
                }
            )
            #print("Columns:", df.columns)
            name_rows = df[df['name'] == 'upload']
            name_rows = name_rows.copy()
            name_rows['value'] = name_rows['value'].astype(str)
            tar_gz_rows = name_rows[name_rows['value'].str.endswith('.tar.gz')]
            print(tar_gz_rows['value'])
            all_tar_gz.extend(tar_gz_rows['value'].tolist())
        # Save to txt file
        with open(output_txt, "w") as f:
            for item in all_tar_gz:
                f.write(f"{item}\n")
        print(f"Saved {len(all_tar_gz)} .tar.gz entries to {output_txt}")

    def download_from_sage(self):
        if not self.portal_user or not self.portal_token:
            print("Portal username or access token not found in config.")
            return

        # URL-encode credentials
        user = urllib.parse.quote_plus(self.portal_user)
        token = urllib.parse.quote_plus(self.portal_token)

        # Load current file list
        if not os.path.exists(self.query_checkpoint):
            print(f"{self.query_checkpoint} not found.")
            return

        with open(self.query_checkpoint, "r") as f:
            current_files = set(line.strip() for line in f if line.strip())

        # Load checkpoint (already downloaded files)
        if os.path.exists(self.download_checkpoint):
            with open(self.download_checkpoint, "r") as f:
                downloaded_files = set(line.strip() for line in f if line.strip())
        else:
            downloaded_files = set()

        os.makedirs(self.download_dir, exist_ok=True)
        os.chdir(self.download_dir)

        new_files = current_files - downloaded_files
        print(f"New files to download: {new_files}")

        for file in new_files:
            print(f"Downloading {file} ...")
            try:
                result = subprocess.run(
                    [
                        "wget",
                        f"--user={user}",
                        f"--password={token}",
                        file
                    ],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                print(result.stdout)
                # Only add to downloaded_files if wget succeeded
                downloaded_files.add(file)
            except subprocess.CalledProcessError as e:
                print(f"Failed to download {file}: {e.stderr}")

        os.chdir(self.parent_dir)
        # Save updated checkpoint
        with open(self.download_checkpoint, "w") as f:
            for file in downloaded_files:
                f.write(f"{file}\n")
        print(f"Checkpoint updated: {self.download_checkpoint}")

    def push_to_cmf(self):
        os.chdir(self.parent_dir)
        #load pushed checkpoint
        if os.path.exists(self.push_checkpoint):
            with open(self.push_checkpoint, "r") as f:
                pushed_list = set(line.strip() for line in f if line.strip())
        else:
            pushed_list = set()
        #load downloaded files
        if os.path.exists(self.download_checkpoint):
            with open(self.download_checkpoint, "r") as f:
                downloaded_list = set(line.strip() for line in f if line.strip())
        else:
            downloaded_list = set()

        #get the list of files to push in cmf_downloads folder
        files_to_push = [
            os.path.join(self.download_dir, os.path.basename(f))
            for f in downloaded_list
            if f.endswith('.tar.gz') and f not in pushed_list
        ]
        print(files_to_push)
        for file in files_to_push:
            #create a child directory based on the file name
            file_name = os.path.basename(file)
            child_dir = os.path.join(self.parent_dir, file_name.replace('.tar.gz', ''))
            print(child_dir)
            os.makedirs(child_dir, exist_ok=True)
            #extract the tar.gz file into the child directory
            with tarfile.open(file, "r:gz") as tar:
                tar.extractall(path=child_dir)
            os.chdir(child_dir)
            #cmf_init
            cmf_init_command_set=self.cmf_init_command
            for key, value in self.cmf_server_config.items():
                placeholder = f"${{{key}}}" #${MINIO_IP}<-> f"${{{MINIO_IP}}}""
                cmf_init_command_set = cmf_init_command_set.replace(placeholder, value)

            # Execute the init command
            try:
                subprocess.run(cmf_init_command_set, shell=True, check=True)
                print("CMF initialization completed successfully.")
            except subprocess.CalledProcessError as e:
                print(f"Error executing CMF initialization: {e}")

            #change source artifact location to dvc-art location
            cmf_instance = cmf.Cmf(self.pipeline_file,self.pipeline_name)
            store=cmf_instance.store
            artifacts=store.get_artifacts()
            for artifact in artifacts:
                if "url" in artifact.properties:
                    url = artifact.properties["url"].string_value
                    if url.startswith(f"{self.pipeline_name}:/src/"): #this is hardcoded for now, need to make it dynamic
                        updated_url = url.replace("/src", "s3://dvc-art", 1) 
                        artifact.properties["url"].string_value = updated_url
                        store.put_artifacts([artifact])
                        print(f"Artifact ID {artifact.id} updated successfully.")
           
            #run the metadata push command
            subprocess.run(f"cmf metadata push -p {self.pipeline_name} -f {self.pipeline_file}", shell=True, check=True)

            # Run the artifact push command
            subprocess.run(f"cmf artifact push -p {self.pipeline_name} -f {self.pipeline_file}", shell=True,check=True)
            
            # Write the completed push entry to pushed checkpoint
            with open(self.push_checkpoint, "a") as f:
                f.write(f"{file}\n")
            
            #go back to the parent directory
            os.chdir(self.parent_dir)
        


if __name__ == "__main__":
    cmfproxy = CmfProxy()
    while True:
        cmfproxy.query_from_sage()
        cmfproxy.download_from_sage()
        cmfproxy.push_to_cmf()
        time.sleep(120)  # Sleep for 2 minutes