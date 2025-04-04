import os

from agent.base import Base
from agent.job import step
import contextlib


class Container(Base):
    def __init__(self, name, server):
        self.name = name
        self.server = server
        self.directory = os.path.join(self.server.containers_directory, name)
        self.config_file = os.path.join(self.directory, "config.json")
        self.image = self.config.get("image")
        if not (os.path.isdir(self.directory) and os.path.exists(self.config_file)):
            raise Exception

    def dump(self):
        return {
            "name": self.name,
            "config": self.config,
        }

    def execute(self, command, input=None):
        return super().execute(command, directory=self.directory, input=input)

    def docker_execute(self, command, input=None):
        interactive = "-i" if input else ""
        command = f"docker exec {interactive} {self.name} {command}"
        return self.execute(command, input=input)

    @step("Start Container")
    def start(self):
        self.create_mount_directories()
        return self.start_container()

    def create_mount_directories(self):
        image = self.config["image"]
        for mount in self.config["mounts"]:
            os.makedirs(mount["source"], exist_ok=True)
            command = (
                "docker run --rm --net none "
                f"-v {mount['source']}:/copymount "
                f"{image} cp -LR {mount['destination']}/. /copymount"
            )
            self.execute(command)

    def start_container(self):
        arguments = self.get_container_args()
        self._inspect_overlay_network()  # Just a swarm quirk
        return self.execute(f"docker run {arguments}")

    def get_container_args(self):
        arguments = [
            f"--name {self.name}",
            "--restart always",
            "--detach",
            f"--hostname {self.name}",
        ]
        for network in self.networks:
            arguments.append(f"--network {network}")
        for mount in self.mounts:
            arguments.append(f"--volume {mount}")
        for port in self.ports:
            arguments.append(f"--publish {port}")
        for variable in self.environment_variables:
            arguments.append(f"--env {variable}")
        arguments.append(self.config["image"])
        return " ".join(arguments)

    def _inspect_overlay_network(self):
        with contextlib.suppress(Exception):
            network = self.config["network"]["name"]
            self.execute(f"docker network inspect {network}")

    @step("Create Overlay Network")
    def create_overlay_network(self):
        subnet_cidr_block = self.config["network"]["subnet_cidr_block"]
        network = self.config["network"]["name"]
        try:
            return self.execute(
                f"docker network create --attachable --subnet {subnet_cidr_block} --driver='overlay' {network}"
            )
        except Exception:
            pass

    @step("Stop Container")
    def stop(self):
        self.execute(f"docker stop {self.name}")
        self.execute(f"ocker rm {self.name}")

    @step("Delete Overlay Network")
    def delete_overlay_network(self):
        network = self.config["network"]
        return self.execute(f"docker network rm {network}")

    @property
    def mounts(self):
        return [f"{mount['source']}:{mount['destination']}" for mount in self.config["mounts"]]

    @property
    def ports(self):
        return [
            (f"{port['host_ip']}:{port['host_port']}:{port['container_port']}")
            for port in self.config["ports"]
        ]

    @property
    def networks(self):
        return [
            f"{network['name']} --ip {network['ip']}" if "ip" in network else network["name"]
            for network in self.config["networks"]
        ]

    @property
    def environment_variables(self):
        return [(f"{key}={value}") for key, value in self.config["environment_variables"].items()]

    @property
    def job_record(self):
        return self.server.job_record

    @property
    def step_record(self):
        return self.server.step_record

    @step_record.setter
    def step_record(self, value):
        self.server.step_record = value
