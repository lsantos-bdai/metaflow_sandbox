from metaflow import FlowSpec, kubernetes, Parameter, step
import subprocess
import os
import shutil

DOCKER_IMAGE = "us-docker.pkg.dev/engineering-380817/batch-processing/bdai_deploy@sha256:04c464acbd7f90cab1946600a23647916f762e73ea47eb7a3c102e847f8e6b13"


class MCAPAnalysisFlow(FlowSpec):
    task_id = Parameter(
        "task_id",
        help="The task ID to query (e.g. '11.22.24_green_cube_on_tray')",
        required=True,
    )

    @step
    def start(self):
        """Process all sessions and organize into a single output structure"""
        print(f"Starting query for task: {self.task_id}")

        # Import and query data platform
        from bdai_tensors.data_platform_location_provider import (
            DataPlatformLocationProvider,
        )

        query = f'WHERE JSON_EXTRACT_SCALAR(extra_json, "$.extra_json.task_id") = "{self.task_id}"'
        location_provider = DataPlatformLocationProvider(
            user_input=query,
            session_only=True,
        )
        locations = location_provider()
        session_ids = location_provider.resolved_keys
        print(f"Sessions found: {session_ids}")

        # Create base output directory with required structure
        self.output_dir = "output"
        for subdir in ["videos", "hdf5", "logs"]:
            os.makedirs(os.path.join(self.output_dir, subdir), exist_ok=True)

        # Create empty training.hdf5
        open(os.path.join(self.output_dir, "hdf5", "training.hdf5"), "a").close()

        from bdai_cli.data_platform.download import download
        from tempfile import TemporaryDirectory

        for session_id in session_ids:
            print(f"Processing session: {session_id}")
            try:
                with TemporaryDirectory() as tmpdir:
                    # Download session data
                    download(session_id, data_local_path=tmpdir, skip_confirmation=True)

                    # Find MCAP file
                    mcap_files = []
                    for root, _, files in os.walk(tmpdir):
                        mcap_files.extend(
                            [
                                os.path.join(root, f)
                                for f in files
                                if f.endswith(".mcap")
                            ]
                        )

                    if not mcap_files:
                        raise Exception("No mcap file found after download")

                    self.mcap_file = mcap_files[0]
                    self.download_path = tmpdir

                    # Extract video and copy all data files
                    self._extract_video()
                    self._copy_data_files()

            except Exception as e:
                print(f"Error processing session {session_id}: {str(e)}")
                raise

        print("Processing complete")
        self.next(self.end)

    @step
    def end(self):
        print("Analysis complete")

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
    MCAPAnalysisFlow()
