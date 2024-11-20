import yaml
import json
import argparse


def get_msg_type(topic_type: str) -> str:
    """Convert topic type to message type."""
    type_mapping = {
        "joint_state": "JointState",
        "pose": "PoseStamped",
        "gripper_state": "FrankaGripperState",
        "rgb": "RGBD",
    }
    return type_mapping.get(topic_type, "Unknown")


def process_robot_config(yaml_data: dict) -> dict:
    """Process robot configuration YAML data."""
    robot_states = {}

    for topic in yaml_data["topics"]:
        name = topic["save_as"] if "save_as" in topic else topic["name"]
        topic_name = topic["name"]
        msg_type = get_msg_type(topic["type"])

        # Remove 'teleop_' prefix from the save_as name if it exists
        state_name = name.replace("teleop_", "") if "teleop_" in name else name

        robot_states[state_name] = {"topic": f"/{topic_name}", "msg_type": msg_type}

    return robot_states


def process_world_config(yaml_data: dict) -> dict:
    """Process world configuration YAML data."""
    world_states = {}

    for topic in yaml_data["topics"]:
        camera_name = topic["save_as"].split("_")[0]
        world_states[camera_name] = {"topic": f"/{topic['name']}", "msg_type": "RGBD"}

    return world_states


def generate_config(
    robot_config_path: str,
    world_config_path: str,
    demo_num: int,
    base_frame: str = "workspace",
    ee_frame: str = "fr3_hand_tcp",
    robot_frame: str = "fr3_link0",
) -> dict:
    """Generate the final configuration dictionary."""

    # Read YAML files
    with open(robot_config_path, "r") as f:
        robot_yaml = yaml.safe_load(f)

    with open(world_config_path, "r") as f:
        world_yaml = yaml.safe_load(f)

    # Create the configuration dictionary
    config = {
        "robot_states": process_robot_config(robot_yaml),
        "world_states": process_world_config(world_yaml),
        "robot_state_frequency": robot_yaml["sampling"]["frequency"],
        "world_state_frequency": world_yaml["sampling"]["frequency"],
        "base_frame": base_frame,
        "ee_frame": ee_frame,
        "robot_frame": robot_frame,
        "demo_num": demo_num,
    }

    return config


def main():
    parser = argparse.ArgumentParser(description="Generate configuration JSON from YAML files")
    parser.add_argument(
        "--robot-config", type=str, default="robot_conf.yaml", help="Path to robot configuration YAML file"
    )
    parser.add_argument(
        "--world-config", type=str, default="world_conf.yaml", help="Path to world configuration YAML file"
    )
    parser.add_argument("--demo-num", type=int, default=0, help="Demo number")
    parser.add_argument("--base-frame", type=str, default="workspace", help="Base frame name")
    parser.add_argument("--ee-frame", type=str, default="fr3_hand_tcp", help="End effector frame name")
    parser.add_argument("--robot-frame", type=str, default="fr3_link0", help="Robot frame name")
    parser.add_argument("--output", type=str, default="config.json", help="Output JSON file path")

    args = parser.parse_args()

    # Generate configuration
    config = generate_config(
        robot_config_path=args.robot_config,
        world_config_path=args.world_config,
        demo_num=args.demo_num,
        base_frame=args.base_frame,
        ee_frame=args.ee_frame,
        robot_frame=args.robot_frame,
    )

    # Print the JSON with proper formatting
    print(json.dumps(config, indent=2))

    # Save to file
    with open(args.output, "w") as f:
        json.dump(config, indent=2, fp=f)


if __name__ == "__main__":
    main()
