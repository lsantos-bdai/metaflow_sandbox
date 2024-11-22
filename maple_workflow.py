from metaflow import FlowSpec, kubernetes, Parameter, step
import subprocess
import os
import shutil


class MCAPAnalysisFlow(FlowSpec):
    # bag_id = Parameter("bag_id", help="The bag ID to download", required=True)
    task_id = Parameter(
        "task_id",
        help="The task ID to query (e.g. '11.22.24_green_cube_on_tray')",
        required=True,
    )

    @step
    def start(self):
        print(f"Starting query for task: {self.task_id}")

        # import dataplatform querying class
        from bdai_tensors.data_platform_location_provider import (
            DataPlatformLocationProvider,
        )

        # Construct the SQL query
        query = f'WHERE JSON_EXTRACT_SCALAR(extra_json, "$.extra_json.task_id") = "{self.task_id}"'

        # Query for all sessions using query parameter
        location_provider = DataPlatformLocationProvider(
            user_input=query,
            session_only=True,
        )

        locations = location_provider()
        print(f"Locations of sessions: {locations}")

        # Save the list of session IDs
        self.session_ids = location_provider.resolved_keys
        print(f"Sessions found: {self.session_ids}")

        # For testing, just move to process_data
        self.next(self.process_data)

    # @step
    # def start(self):
    #     print(f"Starting analysis for bag: {self.bag_id}")
    #
    #     # Create output directories
    #     self.output_dir = "output"
    #     os.makedirs(os.path.join(self.output_dir, "videos"), exist_ok=True)
    #     os.makedirs(os.path.join(self.output_dir, "hdf5"), exist_ok=True)
    #     os.makedirs(os.path.join(self.output_dir, "logs"), exist_ok=True)
    #
    #     try:
    #         from bdai_cli.data_platform.download import download
    #         from tempfile import TemporaryDirectory
    #
    #         with TemporaryDirectory() as tmpdir:
    #             # Download the bag
    #             print("Downloading bag...")
    #             download(
    #                 str(self.bag_id), data_local_path=tmpdir, skip_confirmation=True
    #             )
    #
    #             # Store the downloaded path for processing
    #             self.download_path = tmpdir
    #
    #             # Find and process MCAP file
    #             mcap_files = []
    #             for root, _, files in os.walk(tmpdir):
    #                 mcap_files.extend(
    #                     [os.path.join(root, f) for f in files if f.endswith(".mcap")]
    #                 )
    #
    #             if not mcap_files:
    #                 raise Exception("No mcap file found after download")
    #
    #             self.mcap_file = mcap_files[0]
    #             print(f"Found MCAP file: {self.mcap_file}")
    #
    #             # Extract video
    #             self._extract_video()
    #
    #             # Copy other necessary files
    #             self._copy_data_files()
    #
    #     except Exception as e:
    #         print(f"Error occurred: {str(e)}")
    #         raise
    #
    #     self.next(self.process_data)

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

    @step
    def process_data(self):
        """Process the collected data"""
        # Here you can access:
        # - self.video_output: path to extracted video
        # - self.metadata_json: path to JSON metadata
        # - All files in self.output_dir/hdf5
        # - All files in self.output_dir/logs

        print("Processing collected data...")
        # Add your processing logic here

        self.next(self.end)

    @step
    def end(self):
        print("MCAP analysis complete")


if __name__ == "__main__":
    MCAPAnalysisFlow()
