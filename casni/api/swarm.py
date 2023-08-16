import os
import time
import json
from typing import Optional, Iterator
from dataclasses import dataclass
import docker
from docker.client import DockerClient
from docker.errors import DockerException
from docker.types import Mount
from docker.models.containers import Container as DockerContainer
from docker.models.nodes import Node as DockerNode
from ..helper import colored, message, get_host_address
from .executor import Executor, Task

# ==== DataClasses ====
@dataclass
class Node:
    obj: DockerNode
    client: Optional[DockerClient] = None
    _stream: Optional[Iterator[dict]] = None

    @property
    def id(self):
        """ return unique id of current node """
        return self.obj.attrs['ID']

    @property
    def status(self):
        """ return status dictionary containing state and ip"""
        return self.obj.attrs['Status']
    
    @property
    def state(self):
        """ return state of current node """
        return self.status['State']

    @property
    def ip(self):
        """ return ip of current node """
        return self.status['Addr']

    @property
    def desc(self):
        """ return detailed description of current node """
        return self.obj.attrs['Description']

    @property
    def hostname(self):
        """ return hostname of current node """
        return self.desc['Hostname']

    @property
    def platform(self):
        """ return platform of current node """
        return f'{self.desc["OS"]}-{self.desc["Architecture"]}'
    
    @property
    def installed(self):
        """ list all installed image object in current node """
        return self.client.images.list()

    def is_installed(self, name, tag=None):
        """ check if the image installed in current node """
        installed = [img.tags for img in self.installed]
        if ':' in name:
            search_name, search_tag = name.split(':')
        else:
            search_name = name
            search_tag = tag

        for img_list in installed:
            for img_string in img_list:
                img_name, img_tag = img_string.split(':')
                if img_name == search_name:
                    if search_tag:
                        if img_tag == search_tag:
                            return True
                    return True
        return False

    def pull(self, repository, tag=None):
        self._stream = self.client.api.pull(repository, tag=tag, stream=True, decode=True)

    def get_pull_status(self):
        if self._stream:
            status = dict()
            for line in self._stream:
                if 'id' in line.keys():
                    id = line['id']
                    status[id] = dict()
                    for k, v in line.items():
                        if k != 'id':
                            status[id][k] = v
                yield status

    def remove_image(self, name, *args, **kwargs):
        self.client.images.remove(name, *args, **kwargs)

    @property
    def containers(self):
        if self.client:
            return self.client.containers.list()

    def __repr__(self):
        n_containers = len(self.containers) if self.client else 0
        return f"<Node: '{self.hostname}'[{self.state}] ({self.ip}) (n={n_containers})>"


@dataclass
class Container:
    id: str
    ip: str
    obj: DockerContainer

    def __init__(self, id: str, ip: str, obj: Optional[DockerContainer]):
        self.id = id
        self.ip = ip
        self.obj = obj
    
        # fetch process name
        self.set_idle()
        self._init_event_loop()

    def _init_event_loop(self):
        self._executor = Executor()

    def _init_remote_process(self):
        pass

    @property
    def name(self):
        return self.obj.name

    def submit(self, cmd: str):
        task = Task(function=run_in_container, args=[self, cmd], desc=cmd)
        self._executor.submit(task)

    def fetch_processes(self) -> list:
        return [p[2] for p in self.obj.top()['Processes']]

    def set_idle(self):
        self._processes = self.fetch_processes()

    def usage(self):
        return calc_system_usage(self.obj)

    @property
    def idle(self):
        processes = self.fetch_processes()
        if self._processes == processes:
            return True
        else:
            return False

    @property
    def queue(self):
        return self._executor.queue

    @property
    def outputs(self):
        return self._executor.history
    
    def clear(self):
        self._executor.clear()

    async def wait(self):
        ready = False
        while not ready:
            ready = self.idle

    def stop(self):
        self._executor.stop()
        self.obj.stop()

    def __repr__(self) -> str:
        return f"Container(id: {self.id[:12]}, name: {self.name[:6]}, idle: {self.idle})"

    def __del__(self):
        """terminate the thread
        """
        self.obj.remove()


# ==== Functions ====
def is_manager(client):
    sinfo = client.info()['Swarm']
    if sinfo['NodeID'] in [s['NodeID'] for s in sinfo['RemoteManagers']]:
        return True
    else:
        return False

def get_base_url(ip: str, port: int = 2375) -> str:
    return f"tcp://{ip}:{str(port)}"

def get_ip_address(client: DockerClient) -> dict:
    return client.info()['Swarm']['NodeAddr']

def get_stats_dict(container: DockerContainer) -> dict:
    return json.loads(next(container.stats()).decode('utf-8'))

def calc_system_usage(container: DockerContainer, period_in_second=0.1) -> dict: 
    """
    calculate system usage of input container
    """
    pre = get_stats_dict(container)['cpu_stats']
    num_cpus = pre['online_cpus']
    time.sleep(period_in_second)
    stat = get_stats_dict(container)
    pst = stat['cpu_stats']
    mem = stat['memory_stats']
    
    delta_total = pst['cpu_usage']['total_usage'] - pre['cpu_usage']['total_usage']
    delta_system = pst['system_cpu_usage'] - pre['system_cpu_usage']
    
    cpu_usage_percent = (delta_total / delta_system) * num_cpus * 100
    mem_usage_percent = mem['usage'] / mem['limit'] * 100
    
    return dict(cpu_usage_percent=cpu_usage_percent,
                mem_usage_percent=mem_usage_percent)

def run_in_container(c: Container, cmd: str):
    """
    run cmd on docker container once it became idel status (cpu usage = 0%)
    """
    idle = False
    while not idle:
        idle = c.idle
    return c.obj.exec_run(cmd)


# ==== MainClasses ====
class Manager:
    def __init__(self, n_workers=None, path=None, port=2375):
        self.client = docker.from_env()
        self.port = port
        self.update_nodes()
        self.set_num_workers(n_workers)
        self.service = None
        self.containers = []
        
        if path:
            path = os.path.abspath(path)
            self.volumes = {path:path}
        else:
            self.volumes = None

    @property
    def host_ip(self):
        return get_host_address()

    @property
    def installed(self):
        images  = [n.installed for n in self.nodes]
        return list(set(images[0]).intersection(*images[1:]))

    @property
    def outputs(self):
        outputs = []
        for c in self.containers:
            outputs.extend(c.outputs)
        return outputs

    def is_installed(self, name, tag=None):
        if ':' in name:
            label, tag = name.split(':')
        else:
            label = name
            if not tag:
                tag = 'latest'
        return all([n.is_installed(label, tag) for n in self.nodes])

    def set_num_workers(self, n_workers):
        self.num_workers = n_workers
        if n_workers:
            self.mode = docker.types.ServiceMode('replicated', replicas=n_workers)
        else:
            self.mode = None

    def update_nodes(self):
        self.nodes = []
        for node in self.client.nodes.list():
            node_dc = Node(obj=node)
            if node_dc.state == "ready":
                if node_dc.ip == get_ip_address(self.client):
                    node_dc.client = self.client
                else:
                    try:
                        node_dc.client = docker.DockerClient(base_url=get_base_url(node_dc.ip, port=self.port))
                    except DockerException:
                        message(f'Unable to access node at IP {node_dc.ip}. Ensure `dockerd` is listening on port {self.port}.', io='stderr')
                    except ConnectionRefusedError:
                        message(f'Connection refused for node at IP {node_dc.ip}.', io='stderr')
                    except:
                        message(f'Unexpected error occurred while connecting to node at IP {node_dc.ip}.', io='stderr')
                self.nodes.append(node_dc)

    def create_service(self, image, name, n_workers=None, volumes=None):
        # remove existing service that initiated by this class
        if self.service:
            self.remove_service()
        
        # remove background service with same name
        self.remove_background_services(name)

        # create new service and containers
        if n_workers:
            self.set_num_containers(n_workers)

        mounts = []
        if self.volumes:
            mounts.extend([Mount(target=t, source=s, type='bind') for t, s in self.volumes.items()])
        if volumes:
            mounts.extend([Mount(target=t, source=s, type='bind') for t, s in volumes.items()])
        if len(mounts) == 0:
            mounts = None
        self.service = self.client.services.create(image, name=name, tty=True, mode=self.mode, mounts=mounts)
        self.containers = []

        # + wait until node id appears
        # check if 
        tasks = self.service.tasks()
        assigned = [t for t in tasks if t['Status']['State'] == 'assigned']
        while len(assigned):
            time.sleep(0.1)
            tasks = self.service.tasks()
            assigned = [t for t in tasks if t['Status']['State'] == 'assigned']

        tasks = self.service.tasks()
        errs = [t['Status']['Err'] for t in tasks if t['Status']['State'] == 'rejected']
        if len(errs):
            raise Exception(f'Service rejected: {errs[0]}')

        # + wait until all container active
        tasks = self.service.tasks()
        running = [t for t in tasks if 'ContainerStatus' in t['Status'].keys()]
        print('+ Waiting until service active...', end='')
        while len(running) < len(tasks):
            time.sleep(1)
            tasks = self.service.tasks()
            running = [t for t in tasks if 'ContainerStatus' in t['Status'].keys()]
        print(colored('done', color='blue'))
        
        # + fetch containers from nodes
        for t in self.service.tasks():
            node_id = t['NodeID']
            node = self.client.nodes.get(node_id)
            node_ip = node.attrs['Status']['Addr']
            cont_id = t['Status']['ContainerStatus']['ContainerID']
            node = [n for n in self.nodes if n.id == node_id]
            if len(node):
                client = node[0].client
                if client:
                    cobj = client.containers.get(cont_id)
                    container = Container(id=cont_id, ip=node_ip, obj=cobj)
                    while not container.idle:
                        time.sleep(0.1)
                        container.set_idle()
                    self.containers.append(container)

    def _check_service_stats(self, expect=None, inverse=False):
        output = dict()
        status = [t['Status'] for t in self.service.tasks()]
        output['status'] = status
        if expect:
            print(self.service.tasks(), status)
            output['as_expected'] = [(s['Stats'] == expect) != inverse for s in status]
        return output
    
    def _check_service_key_exists(self, key):
        return [k == key for k in self.service.tasks()]
    
    async def _wait_until_stats_equal_to(self, expect, check_method=all, inverse=False, interval=0.1):
        def check_stats():
            return self._check_service_stats(expect=expect, inverse=inverse)['as_expected']
        stats = check_stats()
        while check_method(stats):
            if interval:
                time.sleep(interval)
            stats = check_stats()

    async def _wait_until_key_exists(self, key):
        key_status = self._check_service_key_exists(key)
        while all(key_status):
            key_status = self._check_service_key_exists(key)

    def remove_service(self):
        # stop all container objects
        if self.containers:
            for c in self.containers:
                c.stop()
            self.containers.clear()

        # remove and clear service
        if self.service:
            self.service.remove()
        self.service = None

    def get_services(self):
        return self.client.services.list()

    def remove_background_services(self, name=None):
        bgservices = [s for s in self.get_services() if s.name != self.service.name] if self.service else self.get_services()
        if name:
            bgservices = [s for s in bgservices if s.name == name]
        if bgservices:
            message("\r+ Terminating background services...")
            for s in bgservices:
                s.remove()
            message(colored("done\n", color='blue'))

    def add_path(self, path):
        self.volumes[path] = path

    def remove_path(self, path):
        del self.volumes[path]

    def search(self, search_term):
        return self.client.images.search(search_term)

    def __del__(self):
        self.remove_service()
    