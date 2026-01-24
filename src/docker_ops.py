import subprocess

def docker_compose_up() -> None:
    subprocess.run(["docker", "compose", "up", "-d"], check=True)
