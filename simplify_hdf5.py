import h5py
import argparse
import numpy as np
from pathlib import Path


def simplify_hdf5(input_path: str, output_path: str):
    """
    Simplify HDF5 file by removing specific fields and adding new ones.
    Also rename fields by removing '_rgb' from camera data fields.
    """
    with h5py.File(input_path, "r") as src, h5py.File(output_path, "w") as dst:
        # Fields to skip
        skip_fields = [
            "workspace_t_camera0_color_optical_frame",
            "workspace_t_camera1_color_optical_frame",
            "workspace_t_camera2_color_optical_frame",
        ]

        # Copy and rename existing fields
        for key in src.keys():
            if key in skip_fields:
                continue

            # Remove '_rgb' from camera data fields
            new_key = key.replace("_rgb_", "_")

            # Copy the dataset
            dst.create_dataset(new_key, data=src[key])

        # Add new empty fields
        dst.create_dataset("keypoint_idxs", data=np.array([], dtype=np.float64))

        # Create workspace_args with the exact values from the example
        workspace_args_content = {
            "bin_counts": [128, 128, 64],
            "surface_thickness_m": 0.019999999552965164,
            "n_slices": 0,
            "bounding_box": [[-0.25, -0.25, 0.01], [0.25, 0.25, 0.25]],
        }

        workspace_args_str = str(workspace_args_content)
        dt = h5py.special_dtype(vlen=str)
        dst.create_dataset("workspace_args", data=workspace_args_str, dtype=dt)


def main():
    parser = argparse.ArgumentParser(description="Simplify HDF5 file structure")
    parser.add_argument("input", type=str, help="Input HDF5 file path")
    parser.add_argument(
        "--output",
        type=str,
        help="Output HDF5 file path (default: input file with _simplified suffix)",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    if args.output:
        output_path = args.output
    else:
        # Create output path by adding _simplified before the extension
        output_path = str(
            input_path.parent / f"{input_path.stem}_simplified{input_path.suffix}"
        )

    simplify_hdf5(args.input, output_path)
    print(f"Simplified HDF5 file saved to: {output_path}")


if __name__ == "__main__":
    main()
