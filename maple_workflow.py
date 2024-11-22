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

        # Fan out to process each session
        self.next(self.process_session, foreach="session_ids")

    @kubernetes(
        image=DOCKER_IMAGE,
        service_account="workflows-team-dc",
        namespace="team-dc",
        persistent_volume_claims={
            "metaflow-pvc": "/mnt/shared"  # Mount PVC at /mnt/shared
        },
    )
    @step
    def process_session(self):
        """Process individual session data"""
        # Current session ID is available as self.input
        session_id = self.input
        print(f"Processing session: {session_id}")

        # Create session-specific output directory in PVC
        self.output_dir = os.path.join("/mnt/shared", f"output_{session_id}")
        print(f"Created output directory in PVC: {self.output_dir}")
        os.makedirs(os.path.join(self.output_dir, "videos"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "hdf5"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "logs"), exist_ok=True)

        # Download and process session
        try:
            from bdai_cli.data_platform.download import download
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as tmpdir:
                print(f"Created temporary directory: {tmpdir}")
                download(session_id, data_local_path=tmpdir, skip_confirmation=True)
                self.download_path = tmpdir
                print(f"Downloaded data to: {self.download_path}")

                # Find MCAP file first
                mcap_files = []
                for root, _, files in os.walk(tmpdir):
                    mcap_files.extend(
                        [os.path.join(root, f) for f in files if f.endswith(".mcap")]
                    )

                if not mcap_files:
                    raise Exception("No mcap file found after download")

                self.mcap_file = mcap_files[0]
                print(f"Found MCAP file: {self.mcap_file}")

                # Now process the downloaded data
                self._extract_video()
                print("Video extraction complete")

                self._copy_data_files()
                print("Data files copying complete")

                # Debug: Print final directory structure
                print("\nFinal directory structure:")
                for root, dirs, files in os.walk(self.output_dir):
                    print(f"\nDirectory: {root}")
                    print(f"Subdirectories: {dirs}")
                    print(f"Files: {files}")

        except Exception as e:
            print(f"Error processing session {session_id}: {str(e)}")
            raise

        print(f"Completed processing for session {session_id}")
        print(f"Output directory contents: {os.listdir(self.output_dir)}")
        self.next(self.join_sessions)

    @step
    def join_sessions(self, inputs):
        """Join results back in local environment"""
        print("Joining session results from Kubernetes jobs")

        # These artifacts will be different for each branch, so exclude them
        exclude_artifacts = [
            "video_output",
            "output_dir",
            "metadata_json",
            "mcap_file",
            "download_path",
        ]

        # Update paths to look in PVC
        for input_obj in inputs:
            pvc_path = os.path.basename(
                input_obj.output_dir
            )  # Get just the session ID part
            pvc_output_dir = os.path.join("/mnt/shared", pvc_path)
            print(f"Processing input with PVC output_dir: {pvc_output_dir}")
            if os.path.exists(pvc_output_dir):
                print(f"Contents of {pvc_output_dir}: {os.listdir(pvc_output_dir)}")
            else:
                print(f"Directory {pvc_output_dir} does not exist!")

        # Update session_dirs to use PVC paths
        self.session_dirs = [
            os.path.join("/mnt/shared", os.path.basename(input_obj.output_dir))
            for input_obj in inputs
        ]

        # Create merged output directory
        self.merged_dir = "merged_output"
        os.makedirs(self.merged_dir, exist_ok=True)

        # Merge artifacts, excluding the ones that are naturally different between branches
        self.merge_artifacts(inputs, exclude=exclude_artifacts)

        self.next(self.merge_data)

    @step
    def merge_data(self):
        """Merge all session data into final structure"""
        print("Starting data merge process")
        print(f"Received session_dirs: {self.session_dirs}")  # Debug print

        # Create base directories
        base_dirs = ["hdf5", "logs", "videos"]
        for dir_name in base_dirs:
            dir_path = os.path.join(self.merged_dir, dir_name)
            os.makedirs(dir_path, exist_ok=True)
            print(f"Created directory: {dir_path}")  # Debug print

        # Debug: Print contents of all session directories
        for session_dir in self.session_dirs:
            print(f"\nInspecting session directory: {session_dir}")
            if os.path.exists(session_dir):
                for root, dirs, files in os.walk(session_dir):
                    print(f"\nDirectory: {root}")
                    print(f"Subdirectories: {dirs}")
                    print(f"Files: {files}")
            else:
                print(f"WARNING: Directory {session_dir} does not exist!")

        # Merge session data
        for session_dir in self.session_dirs:
            if not os.path.exists(session_dir):
                print(f"WARNING: Session directory {session_dir} does not exist")
                continue

            # Get demo dirs from hdf5 directory
            hdf5_path = os.path.join(session_dir, "hdf5")
            if not os.path.exists(hdf5_path):
                print(f"WARNING: HDF5 directory does not exist in {session_dir}")
                continue

            demo_dirs = [
                d
                for d in os.listdir(hdf5_path)
                if os.path.isdir(os.path.join(hdf5_path, d)) and d.startswith("demo_")
            ]
            print(
                f"Found demo directories in {session_dir}: {demo_dirs}"
            )  # Debug print

            for demo_dir in demo_dirs:
                print(f"Processing {demo_dir} from {session_dir}")

                # Copy all directories maintaining structure
                for base_dir in base_dirs:
                    src = os.path.join(session_dir, base_dir, demo_dir)
                    dst = os.path.join(self.merged_dir, base_dir, demo_dir)
                    if os.path.exists(src):
                        print(f"Copying from {src} to {dst}")  # Debug print
                        shutil.copytree(src, dst, dirs_exist_ok=True)
                    else:
                        print(f"WARNING: Source directory does not exist: {src}")

        # Create empty training.hdf5 (placeholder)
        training_file = os.path.join(self.merged_dir, "hdf5", "training.hdf5")
        open(training_file, "a").close()
        print(f"Created empty training file: {training_file}")

        # Final debug print of merged directory structure
        print("\nFinal merged directory structure:")
        for root, dirs, files in os.walk(self.merged_dir):
            print(f"\nDirectory: {root}")
            print(f"Subdirectories: {dirs}")
            print(f"Files: {files}")

        print(f"Merge complete. Final structure in {self.merged_dir}")

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
