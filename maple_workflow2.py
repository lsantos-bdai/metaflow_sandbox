from metaflow import FlowSpec, kubernetes, Parameter, step
import subprocess
import os
import shutil
import csv
import json
import yaml

DOCKER_IMAGE = "us-docker.pkg.dev/engineering-380817/bdai/dc/workflows/maple_test@sha256:3559521456a185d9c4b84c8c927fc8f0cb83db5d1bed843c8f1595f2a98f1b62"
DOCKER_IMAGE_GPU = "us-docker.pkg.dev/engineering-380817/bdai/dc/workflows/maple_test@sha256:7c776756ae225eb745354caeeab5504a3c10c5e70e8172e3913781c3a7fa3b8e"


class MapleWorkflowLinear(FlowSpec):
    task_id = Parameter(
        "query_task_id", type=str, help="The task ID to query", required=True
    )

    # @kubernetes(
    #     image=DOCKER_IMAGE_GPU,
    #     service_account="workflows-team-dc",
    #     namespace="team-dc",
    #     gpu=1,
    #     cpu=1,
    #     node_selector={
    #         "profile": "gpu-ssd"  # Specify GPU type
    #     },
    # )
    # @step
    # def start(self):
    #     """Debug environment"""
    #     from tempfile import TemporaryDirectory
    #     import torch
    #
    #     # Check CUDA availability
    #     print("\nCUDA Setup:")
    #     print(f"CUDA available: {torch.cuda.is_available()}")
    #     if torch.cuda.is_available():
    #         print(f"CUDA device count: {torch.cuda.device_count()}")
    #         print(f"Current CUDA device: {torch.cuda.current_device()}")
    #         print(f"Device name: {torch.cuda.get_device_name()}")
    #
    #     # Verify pytorch3d import
    #     print("\nTesting pytorch3d import...")
    #     try:
    #         import pytorch3d
    #
    #         print(f"pytorch3d version: {pytorch3d.__version__}")
    #     except Exception as e:
    #         print(f"Error importing pytorch3d: {e}")
    #
    #     # Run nvidia-smi if available
    #     print("\nGPU Info:")
    #     try:
    #         subprocess.run(["nvidia-smi"], check=True)
    #     except Exception as e:
    #         print(f"Error running nvidia-smi: {e}")
    #
    #     self.next(self.end)

    # - A100: `@kubernetes(node_selector="gpu-a100-ssd")`
    # - Cheap GPU: `@kubernetes(node_selector="gpu-ssd")`

    @kubernetes(
        image=DOCKER_IMAGE_GPU,
        service_account="workflows-team-dc",
        namespace="team-dc",
        gpu=1,
        cpu=1,
        node_selector={
            "profile": "gpu-ssd"  # Specify GPU type
        },
    )
    @step
    def start(self):
        """Debug environment"""
        from tempfile import TemporaryDirectory

        self.dst = "gs://bdai-common-storage/lsantos/11.22.24_green_cube_on_tray"

        with TemporaryDirectory() as tmpdir:
            print("\nDownloading data...")
            cmd = [
                "gcloud",
                "storage",
                "cp",
                "-r",
                self.dst,
                tmpdir,
            ]
            subprocess.run(cmd, check=True)

            print("\nDirectory contents:")
            self._print_directory_tree(tmpdir)

            print("\nStarting Training")
            os.environ["WANDB_API_KEY"] = "e654b8d65b121602aede3733bba28ba9610407c7"
            download_path = os.path.join(tmpdir, "11.22.24_green_cube_on_tray")
            cmd = [
                "python",
                "/workspaces/bdai/projects/maple/src/equidiff/train.py",
                "--config-name=equi_pointcloud_real",
                f"dataset_path={os.path.join(download_path, 'training_data.hdf5')}",
                "training.num_epochs=1000",
            ]
            subprocess.run(cmd, check=True)

            # Create checkpoints directory in downloaded path
            checkpoint_dir = os.path.join(download_path, "checkpoints")
            os.makedirs(checkpoint_dir, exist_ok=True)

            try:
                result = subprocess.run(
                    [
                        "find",
                        "/workspaces/bdai/projects/maple/src/equidiff/data/outputs",
                        "-name",
                        "latest.ckpt",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )

                checkpoint_path = result.stdout.strip()
                if checkpoint_path:
                    print("\nCopying latest checkpoint...")
                    shutil.copy2(
                        checkpoint_path, os.path.join(checkpoint_dir, "latest.ckpt")
                    )
                    print("Checkpoint copied successfully")
                else:
                    print("Warning: Could not find latest.ckpt")
            except subprocess.CalledProcessError as e:
                print(f"Error finding checkpoint: {str(e)}")

        self.next(self.end)

    # @kubernetes(
    #     image=DOCKER_IMAGE, service_account="workflows-team-dc", namespace="team-dc"
    # )
    # @step
    # def start(self):
    #     """Process all sessions and organize into a single output structure"""
    #     print(f"Starting query for task: {self.task_id}")
    #
    #     # Import and query data platform
    #     from bdai_tensors.data_platform_location_provider import (
    #         DataPlatformLocationProvider,
    #     )
    #
    #     query = f'WHERE JSON_EXTRACT_SCALAR(extra_json, "$.extra_json.task_id") = "{self.task_id}"'
    #     location_provider = DataPlatformLocationProvider(
    #         user_input=query,
    #         session_only=True,
    #     )
    #     locations = location_provider()
    #     session_ids = location_provider.resolved_keys
    #     print(f"Sessions found: {session_ids}")
    #
    #     # Create base output directory with required structure
    #     self.output_dir = "output"
    #     for subdir in ["videos", "hdf5", "logs"]:
    #         os.makedirs(os.path.join(self.output_dir, subdir), exist_ok=True)
    #
    #     from bdai_cli.data_platform.download import download
    #     from bdai_cli.data_platform.upload import upload
    #     from tempfile import TemporaryDirectory
    #
    #     # Create/open demo.csv to store mapping
    #     demo_csv_path = os.path.join(self.output_dir, "demo.csv")
    #     demo_mapping = []
    #
    #     for session_id in session_ids:
    #         print(f"Processing session: {session_id}")
    #         try:
    #             with TemporaryDirectory() as tmpdir:
    #                 # Download session data
    #                 download(session_id, data_local_path=tmpdir, skip_confirmation=True)
    #
    #                 # Find MCAP file
    #                 mcap_files = []
    #                 for root, _, files in os.walk(tmpdir):
    #                     mcap_files.extend(
    #                         [
    #                             os.path.join(root, f)
    #                             for f in files
    #                             if f.endswith(".mcap")
    #                         ]
    #                     )
    #
    #                 if not mcap_files:
    #                     raise Exception("No mcap file found after download")
    #
    #                 self.mcap_file = mcap_files[0]
    #                 self.download_path = tmpdir
    #
    #                 # Get demo number before processing
    #                 source_hdf5 = None
    #                 for root, dirs, _ in os.walk(tmpdir):
    #                     if "hdf5" in dirs:
    #                         source_hdf5 = os.path.join(root, "hdf5")
    #                         break
    #
    #                 if not source_hdf5:
    #                     raise Exception("No hdf5 directory found")
    #
    #                 demo_number = None
    #                 for item in os.listdir(source_hdf5):
    #                     if item.startswith("demo_") and os.path.isdir(
    #                         os.path.join(source_hdf5, item)
    #                     ):
    #                         demo_number = item
    #                         break
    #
    #                 if not demo_number:
    #                     raise Exception("Could not find demo_X directory in hdf5")
    #
    #                 # Add to mapping
    #                 demo_mapping.append([demo_number, session_id])
    #
    #                 # Extract video and copy all data files
    #                 self._extract_video()
    #                 self._copy_data_files()
    #
    #         except Exception as e:
    #             print(f"Error processing session {session_id}: {str(e)}")
    #             raise
    #
    #     # Write demo mapping to CSV
    #     with open(demo_csv_path, "w", newline="") as f:
    #         writer = csv.writer(f)
    #         demo_mapping.sort(key=lambda x: x[0])  # Sort by demo number
    #         for mapping in demo_mapping:
    #             writer.writerow(mapping)
    #
    #     print("Running equidiff data conversion...")
    #     try:
    #         subprocess.run(
    #             [
    #                 "python",
    #                 "/workspaces/bdai/projects/maple/scripts/equidiff/equidiff_data_conversion.py",
    #                 "--source",
    #                 f"{self.output_dir}/hdf5",
    #                 "--output",
    #                 f"{self.output_dir}/training_data.hdf5",
    #                 "--point-cloud",
    #                 "--collected-data",
    #                 "--force",
    #             ],
    #             check=True,
    #         )
    #         print("Data conversion completed successfully")
    #     except subprocess.CalledProcessError as e:
    #         print(f"Error during data conversion: {str(e)}")
    #         raise
    #
    #     # Update metadata.json to set raw=false
    #     metadata_path = os.path.join(self.output_dir, "metadata.json")
    #     with open(metadata_path, "r") as f:
    #         metadata = json.load(f)
    #     metadata["raw"] = False
    #     with open(metadata_path, "w") as f:
    #         json.dump(metadata, f, indent=2)
    #
    #     # print("\nStarting Training")
    #     # os.environ['WANDB_API_KEY'] = "e654b8d65b121602aede3733bba28ba9610407c7"
    #     # cmd = [
    #     #     "python",
    #     #     "/workspaces/bdai/projects/maple/src/equidiff/train.py",
    #     #     "--config-name=equi_pointcloud_real",
    #     #     f"dataset_path={os.path.join(self.output_dir, 'training_data.hdf5')}",
    #     #     "training.num_epochs=1000",
    #     # ]
    #     # subprocess.run(cmd, check=True)
    #     #
    #     # # Create checkpoints directory in output
    #     # os.makedirs(os.path.join(self.output_dir, "checkpoints"), exist_ok=True)
    #     #
    #     # # Find the latest checkpoint using find command
    #     # try:
    #     #     result = subprocess.run(
    #     #         [
    #     #             "find",
    #     #             "/workspaces/bdai/projects/maple/src/equidiff/data/outputs",
    #     #             "-name",
    #     #             "latest.ckpt",
    #     #         ],
    #     #         capture_output=True,
    #     #         text=True,
    #     #         check=True,
    #     #     )
    #     #
    #     #     checkpoint_path = result.stdout.strip()
    #     #     if checkpoint_path:
    #     #         print("\nCopying latest checkpoint...")
    #     #         shutil.copy2(
    #     #             checkpoint_path,
    #     #             os.path.join(self.output_dir, "checkpoints/latest.ckpt"),
    #     #         )
    #     #         print("Checkpoint copied successfully")
    #     #     else:
    #     #         print("Warning: Could not find latest.ckpt")
    #     # except subprocess.CalledProcessError as e:
    #     #     print(f"Error finding checkpoint: {str(e)}")
    #
    #     print("\nUploading processed data...")
    #     self.dst = f"gs://bdai-common-storage/lsantos/{self.task_id}"
    #
    #     cmd = [
    #         "gcloud",
    #         "storage",
    #         "cp",
    #         "-r",
    #         self.output_dir,
    #         self.dst,
    #     ]
    #     subprocess.run(cmd, check=True)
    #     print(f"Data uploaded to {self.dst}")
    #
    #     print("\nProcessing complete")
    #     print("\nFinal Directory Structure:")
    #     print("output/")
    #     self._print_directory_tree("output")
    #
    #     print("\nDemo CSV Contents:")
    #     with open(os.path.join(self.output_dir, "demo.csv"), "r") as f:
    #         print(f.read())
    #
    #     self.next(self.end)

    @step
    def end(self):
        print("Analysis complete")

    def _print_directory_tree(self, startpath):
        """Create a visual tree representation of the directory structure"""
        for root, dirs, files in os.walk(startpath):
            level = root.replace(startpath, "").count(os.sep)
            indent = "│   " * (level - 1) + "├── " if level > 0 else ""
            print(f"{indent}{os.path.basename(root)}/")
            subindent = "│   " * level + "├── "
            for f in files:
                print(f"{subindent}{f}")

    def _extract_video(self):
        image_topic = "/camera/camera1/color/image_raw"
        # Store in a temporary location first
        self.video_output = os.path.join(self.output_dir, "temp_video.mp4")
        print(f"Extracting video to: {self.video_output}")
        subprocess.run(
            [
                "video_ripper_cli",
                "--image-topic",
                image_topic,
                self.mcap_file,
                self.video_output,
            ],
            check=True,
        )

    def _copy_data_files(self):
        def find_directory(dir_name):
            for root, dirs, _ in os.walk(self.download_path):
                if dir_name in dirs:
                    return os.path.join(root, dir_name)
            return None

        # First, find the hdf5 directory and determine the demo number
        source_hdf5 = find_directory("hdf5")
        if not source_hdf5:
            raise Exception("No hdf5 directory found")

        # Get the parent directory of hdf5
        parent_dir = os.path.dirname(source_hdf5)

        # Get demo number from hdf5 directory - look specifically for directories
        demo_number = None
        for item in os.listdir(source_hdf5):
            if item.startswith("demo_") and os.path.isdir(
                os.path.join(source_hdf5, item)
            ):
                demo_number = item
                break

        if not demo_number:
            raise Exception("Could not find demo_X directory in hdf5")

        print(f"Found {demo_number} in hdf5 directory")

        # Create the output structure
        os.makedirs(os.path.join(self.output_dir, "hdf5", demo_number), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "logs", demo_number), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "videos", demo_number), exist_ok=True)

        # Copy HDF5 files maintaining structure
        shutil.copytree(
            os.path.join(source_hdf5, demo_number),
            os.path.join(self.output_dir, "hdf5", demo_number),
            dirs_exist_ok=True,
        )

        # Copy demo metadata json if it exists
        demo_metadata = f"{demo_number}_metadata.json"
        metadata_source = os.path.join(source_hdf5, demo_metadata)
        if os.path.isfile(metadata_source):
            shutil.copy2(
                metadata_source, os.path.join(self.output_dir, "hdf5", demo_metadata)
            )

        # Copy logs into demo structure
        source_logs = find_directory("logs")
        if source_logs:
            for log_file in os.listdir(source_logs):
                if log_file.endswith(".log"):
                    shutil.copy2(
                        os.path.join(source_logs, log_file),
                        os.path.join(self.output_dir, "logs", demo_number, log_file),
                    )

        # Move video to correct demo structure
        if hasattr(self, "video_output") and os.path.exists(self.video_output):
            new_video_path = os.path.join(
                self.output_dir, "videos", demo_number, "video.mp4"
            )
            shutil.move(self.video_output, new_video_path)
            self.video_output = new_video_path

        # Find and copy JSON file from the parent directory
        for file in os.listdir(parent_dir):
            if file.endswith(".json"):
                self.metadata_json = os.path.join(self.output_dir, "metadata.json")
                shutil.copy2(os.path.join(parent_dir, file), self.metadata_json)
                break
        else:
            print("Warning: Could not find JSON file in the expected location")


if __name__ == "__main__":
    MapleWorkflowLinear()
