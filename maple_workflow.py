from metaflow import FlowSpec, kubernetes, Parameter, step
import subprocess
import os


class MCAPAnalysisFlow(FlowSpec):
    bag_id = Parameter("bag_id", help="The bag ID to download", required=True)
    robot_conf = Parameter("robot_conf", help="Path to robot configuration YAML", default="config/robot_conf.yaml")
    world_conf = Parameter("world_conf", help="Path to world configuration YAML", default="config/world_conf.yaml")

    @step
    def start(self):
        print(f"Starting analysis for bag: {self.bag_id}")

        # Create output directories if they don't exist
        os.makedirs("output/videos", exist_ok=True)
        os.makedirs("output/hdf5", exist_ok=True)

        try:
            from bdai_cli.data_platform.download import download
            from tempfile import TemporaryDirectory

            # Download the bag using API
            print("Downloading bag...")
            with TemporaryDirectory() as tmpdir:
                download(str(self.bag_id), data_local_path=tmpdir, skip_confirmation=True)

                # Find the downloaded mcap file recursively
                mcap_files = []
                for root, _, files in os.walk(tmpdir):
                    mcap_files.extend([os.path.join(root, f) for f in files if f.endswith(".mcap")])

                if not mcap_files:
                    raise Exception("No mcap file found after download")

                self.mcap_file = mcap_files[0]
                print(f"Found MCAP file: {self.mcap_file}")

                # Extract video from mcap file
                image_topic = "/camera/camera1/color/image_raw"
                video_output = os.path.join("output", "videos", "output.mp4")
                print(f"Extracting video to: {video_output}")
                subprocess.run(
                    ["video_ripper_cli", "--image-topic", image_topic, self.mcap_file, video_output], check=True
                )

                # Convert to robot state HDF5
                robot_output = os.path.join("output", "hdf5", "robot_state.hdf5")
                print(f"Converting to robot state HDF5: {robot_output}")
                subprocess.run(
                    [
                        "ros2",
                        "run",
                        "bdai_ros",
                        "mcap2hdf5.py",
                        "--bagfile",
                        self.mcap_file,
                        "--config",
                        self.robot_conf,
                        "--output",
                        robot_output,
                    ],
                    check=True,
                )

                # Convert to world state HDF5
                world_output = os.path.join("output", "hdf5", "world_state.hdf5")
                print(f"Converting to world state HDF5: {world_output}")
                subprocess.run(
                    [
                        "ros2",
                        "run",
                        "bdai_ros",
                        "mcap2hdf5.py",
                        "--bagfile",
                        self.mcap_file,
                        "--config",
                        self.world_conf,
                        "--output",
                        world_output,
                    ],
                    check=True,
                )

        except Exception as e:
            print(f"Error occurred: {str(e)}")
            raise

        self.next(self.end)

    @step
    def end(self):
        print("MCAP analysis complete")


if __name__ == "__main__":
    MCAPAnalysisFlow()
