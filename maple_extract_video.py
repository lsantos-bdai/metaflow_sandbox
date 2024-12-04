from metaflow import FlowSpec, kubernetes, Parameter, step
import subprocess
import os
import shutil

DOCKER_IMAGE_GPU = (
    "us-docker.pkg.dev/engineering-380817/bdai/dc/workflows/maple_test:metaflow_v2_gpu"
)


class MapleVideoExtract(FlowSpec):
    session_id = Parameter(
        "session_id", type=str, help="rosbag session_id", required=True
    )
    task_id = Parameter(
        "query_task_id", type=str, help="The task ID to upload to", required=True
    )

    @kubernetes(
        image=DOCKER_IMAGE_GPU,
        service_account="workflows-team-dc",
        namespace="team-dc",
        cpu=1,
    )
    @step
    def start(self):
        """Process all sessions and organize into a single output structure"""
        print(f"Starting Video Extraction: {self.session_id}")

        # Create base output directory with required structure
        self.output_dir = "output"
        os.makedirs(os.path.join(self.output_dir), exist_ok=True)

        from bdai_cli.data_platform.download import download
        from tempfile import TemporaryDirectory

        print(f"Processing session: {self.session_id}")
        try:
            with TemporaryDirectory() as tmpdir:
                # Download session data
                download(
                    str(self.session_id), data_local_path=tmpdir, skip_confirmation=True
                )

                # Find MCAP file
                mcap_files = []
                for root, _, files in os.walk(tmpdir):
                    mcap_files.extend(
                        [os.path.join(root, f) for f in files if f.endswith(".mcap")]
                    )

                if not mcap_files:
                    raise Exception("No mcap file found after download")

                self.mcap_file = mcap_files[0]
                self.download_path = tmpdir

                # Extract video and copy all data files
                self._extract_video()

        except Exception as e:
            print(f"Error processing session {self.session_id}: {str(e)}")
            raise

        print("\nUploading processed data...")
        # self.dst = f"gs://bdai-common-storage/lsantos/{self.task_id}/videos/"
        self.dst = f"gs://project-maple-main-storage/data/HIL_test/{self.task_id}/videos/"

        cmd = [
            "gcloud",
            "storage",
            "cp",
            "-r",
            self.video_output,
            self.dst,
        ]
        subprocess.run(cmd, check=True)
        print(f"Data uploaded to {self.dst}")

        print("\nProcessing complete")
        print("\nFinal Directory Structure:")
        print("output/")
        self._print_directory_tree("output")

        self.next(self.end)

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
        self.video_output = os.path.join(self.output_dir, "output_behavior.mp4")
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


if __name__ == "__main__":
    MapleVideoExtract()
