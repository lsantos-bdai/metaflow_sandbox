from metaflow import FlowSpec, kubernetes, Parameter, step
import subprocess
import os
import shutil
import json

DOCKER_IMAGE_GPU = (
    "us-docker.pkg.dev/engineering-380817/bdai/dc/workflows/maple_test:metaflow_v2_gpu"
)


class TrainFromHDF5(FlowSpec):
    task_query_id = Parameter("task_query_id", type=str, help="task_id to train", required=True)

    @kubernetes(
        image=DOCKER_IMAGE_GPU,
        service_account="workflows-team-dc",
        namespace="team-dc",
        gpu=1,
        cpu=1,
        node_selector={"profile": "gpu-a100-ssd"},
    )
    @step
    def start(self):
        import torch

        # Query data platform
        from bdai_tensors.data_platform_location_provider import (
            DataPlatformLocationProvider,
        )

        query = f'WHERE JSON_EXTRACT_SCALAR(extra_json, "$.extra_json.task_id") = "{self.task_query_id}"'
        location_provider = DataPlatformLocationProvider(
            user_input=query,
            session_only=True,
        )
        locations = location_provider()
        session_ids = location_provider.resolved_keys

        if len(session_ids) > 1:
            raise Exception(
                f"Multiple sessions found for task_id {self.task_query_id}. Expected only one."
            )
        if len(session_ids) == 0:
            raise Exception(f"No sessions found for task_id {self.task_query_id}")

        session_id = session_ids[0]
        print(f"Processing session: {session_id}")

        # Download session data
        from bdai_cli.data_platform.download import download
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            download(session_id, data_local_path=tmpdir, skip_confirmation=True)
            self.download_path = tmpdir

            # Set up output directory
            self.output_dir = "output"
            os.makedirs(os.path.join(self.output_dir, "hdf5"), exist_ok=True)
            os.makedirs(os.path.join(self.output_dir, "checkpoints"), exist_ok=True)

            # Find the task directory (should be the only subdirectory except metadata.json)
            task_dir = next(
                d for d in os.listdir(tmpdir) if os.path.isdir(os.path.join(tmpdir, d))
            )
            task_path = os.path.join(tmpdir, task_dir)

            # Find the experiment directory (should be the only subdirectory)
            exp_dir = next(
                d
                for d in os.listdir(task_path)
                if os.path.isdir(os.path.join(task_path, d))
            )
            exp_path = os.path.join(task_path, exp_dir)

            # Find and copy the DC json from the experiment directory
            dc_json = next(f for f in os.listdir(exp_path) if f.endswith(".json"))
            shutil.copy2(
                os.path.join(exp_path, dc_json), os.path.join(self.output_dir, dc_json)
            )

            # Copy HDF5 data
            source_hdf5 = os.path.join(exp_path, "hdf5")
            shutil.copytree(
                source_hdf5, os.path.join(self.output_dir, "hdf5"), dirs_exist_ok=True
            )

        print("Running equidiff data conversion...")
        subprocess.run(
            [
                "python",
                "/workspaces/bdai/projects/maple/scripts/equidiff/equidiff_data_conversion.py",
                "--source",
                f"{self.output_dir}/hdf5",
                "--output",
                f"{self.output_dir}/training_data.hdf5",
                "--point-cloud",
                "--collected-data",
                "--force",
            ],
            check=True,
        )

        # Copy training_data.hdf5 to hdf5 directory
        shutil.copy2(
            os.path.join(self.output_dir, "training_data.hdf5"),
            os.path.join(self.output_dir, "hdf5", "training_data.hdf5"),
        )

        os.environ["WANDB_API_KEY"] = "e654b8d65b121602aede3733bba28ba9610407c7"
        print("Starting Training")
        subprocess.run(
            [
                "python",
                "/workspaces/bdai/projects/maple/src/equidiff/train.py",
                "--config-name=equi_pointcloud_real",
                f"dataset_path={os.path.join(self.output_dir, 'training_data.hdf5')}",
                "training.num_epochs=1000",
            ],
            check=True,
        )

        # Copy checkpoint
        checkpoint_path = subprocess.run(
            ["find", "/metaflow/data", "-name", "latest.ckpt"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

        if checkpoint_path:
            shutil.copy2(
                checkpoint_path,
                os.path.join(self.output_dir, "checkpoints/latest.ckpt"),
            )

        # Upload to GCS
        dst = f"gs://project-maple-main-storage/data/HIL_test/{self.task_query_id}"
        subprocess.run(
            ["gcloud", "storage", "cp", "-r", self.output_dir, dst], check=True
        )

        self.next(self.end)

    @step
    def end(self):
        print("Analysis complete")


if __name__ == "__main__":
    TrainFromHDF5()
