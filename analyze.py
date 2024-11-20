#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path
import h5py
import numpy as np


def analyze_hdf5(file_path: Path) -> None:
    """
    Analyze and print out the contents of an HDF5 file.

    Args:
        file_path (Path): Path to the HDF5 file to analyze
    """
    try:
        with h5py.File(file_path, "r") as f:
            print(f"\nHDF5 File: {file_path}")
            print("\nStructure:")
            print_structure(f)

            # # Detailed analysis of camera data if present
            # if "camera0_rgb" in f:
            #     print("\nDetailed Camera Data Analysis:")
            #     analyze_camera_data(f)
    except OSError as e:
        print(f"Error opening file {file_path}: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error analyzing {file_path}: {e}", file=sys.stderr)
        sys.exit(1)


def analyze_camera_data(f: h5py.File) -> None:
    """
    Perform detailed analysis of camera data in the HDF5 file.

    Args:
        f (h5py.File): Open HDF5 file handle
    """
    # Analyze RGB data
    if "camera0_rgb" in f:
        rgb_data = f["camera0_rgb"]
        print("\nRGB Data Analysis:")
        print(f"  Number of frames: {len(rgb_data)}")
        print(f"  Size of first frame: {len(rgb_data[0])} bytes")

        # Sample the first frame data
        first_frame = rgb_data[0]
        print(
            f"  First frame unique bytes: {len(np.unique(np.frombuffer(first_frame)))}"
        )
        print(
            f"  First frame data range: [{min(np.frombuffer(first_frame))}, {max(np.frombuffer(first_frame))}]"
        )

    # Analyze Depth data
    if "camera0_depth" in f:
        depth_data = f["camera0_depth"]
        print("\nDepth Data Analysis:")
        print(f"  Number of frames: {len(depth_data)}")
        print(f"  Size of first frame: {len(depth_data[0])} bytes")

        # Sample the first frame data
        first_frame = depth_data[0]
        print(
            f"  First frame unique bytes: {len(np.unique(np.frombuffer(first_frame)))}"
        )
        print(
            f"  First frame data range: [{min(np.frombuffer(first_frame))}, {max(np.frombuffer(first_frame))}]"
        )

    # Compare timestamps
    if "timestamps" in f:
        print("\nTimestamp Analysis:")
        timestamps = f["timestamps"][:]
        print(f"  Number of timestamps: {len(timestamps)}")
        print(f"  Timestamp range: [{min(timestamps)}, {max(timestamps)}]")
        print(
            f"  Average time between frames: {np.mean(np.diff(timestamps))/1e9:.3f} seconds"
        )


def print_structure(item: h5py.Group, indent: str = "") -> None:
    """
    Recursively print the HDF5 file structure.

    Args:
        item (h5py.Group): HDF5 group to analyze
        indent (str): Current indentation level for pretty printing
    """
    for key, val in item.items():
        print(f"{indent}{key}:")
        if isinstance(val, h5py.Group):
            print_structure(val, indent + "  ")
        elif isinstance(val, h5py.Dataset):
            print(f"{indent}  Shape: {val.shape}, Dtype: {val.dtype}")


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Analyze the structure of HDF5 files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("files", type=Path, nargs="+", help="HDF5 files to analyze")
    parser.add_argument(
        "--compare", action="store_true", help="Compare multiple files for differences"
    )

    return parser.parse_args()


def main() -> None:
    """Main function to run the HDF5 analysis."""
    args = parse_arguments()

    for file_path in args.files:
        if not file_path.exists():
            print(f"Error: File {file_path} does not exist", file=sys.stderr)
            continue

        if not file_path.is_file():
            print(f"Error: {file_path} is not a file", file=sys.stderr)
            continue

        analyze_hdf5(file_path)

    # If comparing multiple files
    if args.compare and len(args.files) > 1:
        print("\nComparison Analysis:")
        datasets = {}
        for file_path in args.files:
            with h5py.File(file_path, "r") as f:
                for key in f.keys():
                    if key not in datasets:
                        datasets[key] = []
                    datasets[key].append((file_path.name, f[key].shape, f[key].dtype))

        # Print differences
        for key, values in datasets.items():
            if len(set((shape, dtype) for _, shape, dtype in values)) > 1:
                print(f"\nDifferences found in dataset: {key}")
                for fname, shape, dtype in values:
                    print(f"  {fname}: Shape {shape}, Dtype {dtype}")


if __name__ == "__main__":
    main()
