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
        return self.execute(f"docker run {arguments}")

    def get_container_args(self):
        network = self.config["network"]["name"]
        ip = self.config["network"]["ip_address"]
        arguments = [
            f"--name {self.name}",
            "--restart always",
            "--detach",
            f"--hostname {self.name}",
            f"--network {network} --ip {ip}",
        ]
        for mount in self.mounts:
            arguments.append(f"--volume {mount}")
        for port in self.ports:
            arguments.append(f"--publish {port}")
        for variable in self.environment_variables:
            arguments.append(f"--env {variable}")
        arguments.append(self.config["image"])
        return " ".join(arguments)

    @step("Create Network")
    def create_network(self):
        subnet_cidr_block = self.config["network"]["subnet_cidr_block"]
        network = self.config["network"]["name"]
        try:
            return self.execute(
                f"docker network create --attachable --subnet {subnet_cidr_block} --driver='bridge' {network}"
            )
        except Exception:
            pass

    @step("Stop Container")
    def stop(self):
        self.execute(f"docker stop {self.name}")
        self.execute(f"docker rm {self.name}")

    @step("Delete Network")
    def delete_network(self):
        network = self.config["network"]
        try:
            return self.execute(f"docker network rm {network}")
        except Exception:
            pass

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
        return [f"{network['name']} --ip {network['ip']}" for network in self.config["networks"]]

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
