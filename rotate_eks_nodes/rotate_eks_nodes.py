import datetime
import select
import subprocess
import time
from sys import argv
import sys
import os
from dataclasses import dataclass
from typing import Dict, DefaultDict
from collections import defaultdict
import logging
import boto3
import kubernetes
from kubernetes.client.rest import ApiException


@dataclass
class ProcessOutput:
  stderr: str
  stdout: str
  other_fds: Dict[int, str]
  proc: subprocess.Popen


def stream_while_running(cwd, args, line_delimiter='', log_output=True):
  """Helper function for streaming a process' output whilst gathering it into a string"""
  proc = subprocess.Popen(cwd=cwd,
                          args=args, stderr=subprocess.PIPE,
                          stdout=subprocess.PIPE, text=True, bufsize=1)

  stderr = []
  stdout = []
  fds: DefaultDict[str] = defaultdict(list)
  while True:
    poll = proc.poll()
    if poll is not None:
      break
    else:
      # Poll the stdout/err streams every 0.05 seconds to see if anything has been written
      rdrs, _, _ = select.select([proc.stdout, proc.stderr], [], [], 0.05)

      # figure out which was returned
      if rdrs != []:
        for r in rdrs:
          # The FD might return multiple lines, so keep calling readline()
          # until we get a '' implying nothing more to consume
          keep_reading = True
          while keep_reading:
            line = r.readline()
            if len(line) > 0:
              if line[-1] == "\n":
                line = line[:-1]
              if r.name == proc.stdout.fileno():
                stdout.append(line)
                if log_output:
                  #logging.info(f"PID {str(proc.pid)} stdout: {line}")
                  logging.info(f"{line}")
              elif r.name == proc.stderr.fileno():
                stderr.append(line)
                if log_output:
                  #logging.info(f"PID {str(proc.pid)} stderr: {line}")
                  logging.info(f"{line}")
              else:
                fds[r.name].append(line)
                if log_output:
                  #logging.info(f"PID {str(proc.pid)} fd {r.name}: {line}")
                  logging.info(f"{line}")
            else:
              keep_reading = False
      else:
        # no new output
        continue

  # Flatten the list of list of strings that are the file descriptor outputs.
  # It makes it consistent with the subprocess output capture format
  for fd in fds:
    fds[fd] = line_delimiter.join(fds[fd])

  return ProcessOutput(line_delimiter.join(stderr), line_delimiter.join(stdout), fds, proc)


def slurp_ec2_instances(client):
  next_token = None
  instances = []
  keep_going = True
  while keep_going:
    if next_token is None:
      tmp = client.describe_instances()
    else:
      tmp = client.describe_instances(NextToken=next_token)
    for reservation in tmp['Reservations']:
      for instance in reservation['Instances']:
        instances.append(instance)
      if 'NextToken' in tmp and tmp['NextToken'] is not None:
        next_token = tmp['NextToken']
      else:
        keep_going = False
  return instances


def get_asgs_names(client, target):
  next_token = None
  asgs_names_list = []
  keep_going = True
  while keep_going:
    if next_token is None:
      tmp = client.describe_auto_scaling_groups()
    else:
      tmp = client.describe_auto_scaling_groups(NextToken=next_token)
    for asg in tmp['AutoScalingGroups']:
      if target == 'dbs':
        if 'db' in asg['AutoScalingGroupName']:
          asgs_names_list.append(asg['AutoScalingGroupName'])
      elif target == 'apps':
        if 'db' not in asg['AutoScalingGroupName']:
          asgs_names_list.append(asg['AutoScalingGroupName'])
      elif target == 'all':
        asgs_names_list.append(asg['AutoScalingGroupName'])
    if 'NextToken' in tmp and tmp['NextToken'] is not None:
      next_token = tmp['NextToken']
    else:
      keep_going = False
  return asgs_names_list


def main(region, target):

  logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(asctime)s: %(message)s'
  )

  ec2_client = boto3.client('ec2', region_name=region)
  asg_client = boto3.client('autoscaling', region_name=region)

  namespace = 'default'
  snooze_seconds = 30
  pods_to_ignore = ['datadog-agent']
  errored = False

  # Get ASGS
  asgs_names_list = get_asgs_names(asg_client, target)

  # Get K8s nodes information
  kubernetes.config.load_kube_config()
  k8s_core_client = kubernetes.client.CoreV1Api()
  nodes = k8s_core_client.list_node()

  # Get EC2 instance tag information
  ec2_instances = slurp_ec2_instances(ec2_client)

  for k8s_node in nodes.items:
    instance_id = k8s_node.spec.provider_id.split('/')[-1]
    node_name = k8s_node.metadata.name
    matched_ec2_node = list(filter(lambda x: x['InstanceId'] == instance_id, ec2_instances))[0]
    asg_name = [x['Value'] for x in matched_ec2_node['Tags'] if x['Key'] == 'aws:autoscaling:groupName'][0]
    if asg_name not in asgs_names_list:
      continue

    print()
    logging.info(f"Dealing with instance: {instance_id}, in ASG: {asg_name}")

    logging.info(f"Will cordon instance: {instance_id}, with node_name: {node_name} so no new pods are scheduled on it")
    stream_while_running("/", ['kubectl', 'cordon', node_name])

    # Create a dict with node and pods details
    try:
      node_and_pods_dict = {}
      kubernetes.config.load_kube_config()
      k8s_core_client = kubernetes.client.CoreV1Api()
      field_selector = 'spec.nodeName=' + k8s_node.metadata.name
      ret = k8s_core_client.list_namespaced_pod(namespace, watch=False, field_selector=field_selector)
      node_and_pods_dict[instance_id] = {}
      node_and_pods_dict[instance_id]['node_name'] = k8s_node.metadata.name
      node_and_pods_dict[instance_id]['asg_name'] = asg_name
      node_and_pods_dict[instance_id]['pods'] = []

      for i in ret.items:
        if not i.metadata.name.startswith(tuple(pods_to_ignore)):
          node_and_pods_dict[instance_id]['pods'].append(i.metadata.name)
      node_and_pods_dict[instance_id]['number_of_pods'] = len(node_and_pods_dict[instance_id]['pods'])

    except ApiException as e:
      logging.error("Exception when calling Kubernetes CoreV1Api->list_namespaced_pod: %s\n" % e)

    logging.info(f"Instance and pod info: {node_and_pods_dict}")

    pod_controller_and_controller_kind_tuple_list = []

    # Create a list of tuples with controller name (derived from pod name) and controller kind
    for pod in node_and_pods_dict[instance_id]['pods']:
      try:
        kubernetes.config.load_kube_config()
        k8s_core_client = kubernetes.client.CoreV1Api()
        api_response = k8s_core_client.read_namespaced_pod(pod, namespace)
        controller_kind = (api_response.__dict__['_metadata'].__dict__['_owner_references'][0].__dict__['_kind'])
        pod_controller = pod.rpartition('-')[0]
        pod_controller_and_controller_kind_tuple = (pod_controller, controller_kind)
        pod_controller_and_controller_kind_tuple_list.append(pod_controller_and_controller_kind_tuple)
      except ApiException as e:
        logging.error("Exception when calling Kubernetes CoreV1Api->read_namespaced_pod: %s\n" % e)

    # Scale up ASG DesiredCapacity by 1, and also possibly increase MaxSize by 1 if DesiredCapacity + 1 > MaxSize
    logging.info(f"Dealing with ASG: {asg_name}")
    get_asg = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])

    # Current ASG values
    current_desired_capacity = get_asg['AutoScalingGroups'][0]['DesiredCapacity']
    logging.info(f"Current ASG DesiredCapacity: {current_desired_capacity}")
    current_max_size = get_asg['AutoScalingGroups'][0]['MaxSize']
    logging.info(f"Current ASG MaxSize: {current_max_size}")

    # New ASG values
    new_desired_capacity = current_desired_capacity + 1
    logging.info(f"New ASG DesiredCapacity: {new_desired_capacity}")
    new_max_size = new_desired_capacity if new_desired_capacity > current_max_size else current_max_size
    logging.info(f"New ASG MaxSize: {new_max_size}")

    try:
      logging.info(f"Updating ASG: {asg_name} with new DesiredCapacity: {new_desired_capacity} and MaxSize: {new_max_size}")
      update_asg = asg_client.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=new_desired_capacity, MaxSize=new_max_size)

      # Wait for new instance to be InService
      logging.info("Waiting for new instance to be InService...")
      new_instance_not_up = True
      while new_instance_not_up:
        get_asg = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
        instances = get_asg['AutoScalingGroups'][0]['Instances']
        instances_lifecycle_states = [instance['LifecycleState'] for instance in instances]
        number_of_in_service_instances = instances_lifecycle_states.count("InService")
        if number_of_in_service_instances == new_desired_capacity:
          logging.info("New instance ready. Moving along...")
          new_instance_not_up = False
        else:
          logging.info(f"Still waiting for new instance to be InService. Will snooze for {snooze_seconds} seconds and try again...")
          time.sleep(snooze_seconds)

        # Set a 5 minute limit on the startup, in case something goes wrong
        start_time = datetime.datetime.now()
        if datetime.datetime.now() > (start_time + datetime.timedelta(minutes=5)):
          raise RuntimeError("Failed to get new node in 5 minutes. Bailing out!")

      # Drain the old node, and wait till it's fully drained
      logging.info(f"Draining old instance {instance_id}/{node_name} in ASG {asg_name}")
      stream_while_running("/", ['kubectl', 'drain', '--ignore-daemonsets', '--delete-local-data', node_name])
      all_pods_not_drained = True

      while all_pods_not_drained:
        try:
          node_and_pods_drained_node_dict = {}
          kubernetes.config.load_kube_config()
          k8s_core_client = kubernetes.client.CoreV1Api()
          field_selector = 'spec.nodeName=' + k8s_node.metadata.name
          ret = k8s_core_client.list_namespaced_pod(namespace, watch=False, field_selector=field_selector)
          node_and_pods_drained_node_dict[instance_id] = {}
          node_and_pods_drained_node_dict[instance_id]['node_name'] = k8s_node.metadata.name
          node_and_pods_drained_node_dict[instance_id]['asg_name'] = asg_name
          node_and_pods_drained_node_dict[instance_id]['pods'] = []

          for i in ret.items:
            if not i.metadata.name.startswith(tuple(pods_to_ignore)):
              node_and_pods_drained_node_dict[instance_id]['pods'].append(i.metadata.name)
          node_and_pods_drained_node_dict[instance_id]['number_of_pods'] = len(node_and_pods_drained_node_dict[instance_id]['pods'])

        except ApiException as e:
          logging.error("Exception when calling Kubernetes CoreV1Api->list_namespaced_pod: %s\n" % e)

        if node_and_pods_drained_node_dict[instance_id]['number_of_pods'] == 0:
          logging.info(f"All pods have been drained off instance {instance_id}. Moving along...")
          all_pods_not_drained = False
        else:
          logging.info(f"Still waiting for all pods to be drained off instance {instance_id}. {node_and_pods_drained_node_dict[instance_id]['number_of_pods']} pod(s) ({node_and_pods_drained_node_dict[instance_id]['pods']}) left. Will snooze for {snooze_seconds} seconds and try again...")
          time.sleep(snooze_seconds)

      # Wait and keep checking Kubernetes API to see if there are 100% replicas available for each ReplicaSet and StatefulSet
      logging.info(f"Will wait for all replicas to be available for each ReplicaSet/StatefulSet, while ignoring DaemonSet/Ad Hoc Pods ({pods_to_ignore})...")

      set_counter_list = []

      while len(pod_controller_and_controller_kind_tuple_list) > len(set_counter_list):
        for pod_controller, controller_kind in pod_controller_and_controller_kind_tuple_list:
          logging.info(f"Dealing with {pod_controller}, {controller_kind}")
          all_replicas_not_available = True
          while all_replicas_not_available:
            try:
              kubernetes.config.load_kube_config()
              k8s_app_client = kubernetes.client.AppsV1Api()
              if controller_kind == 'ReplicaSet':
                api_response = k8s_app_client.read_namespaced_replica_set(pod_controller, namespace)
              elif controller_kind == 'StatefulSet':
                api_response = k8s_app_client.read_namespaced_stateful_set(pod_controller, namespace)
              else:
                set_counter_list.append("UnknownController")
                continue
              number_of_replicas = api_response.__dict__['_status'].__dict__['_replicas']
              number_of_ready_replicas = api_response.__dict__['_status'].__dict__['_ready_replicas']
              if number_of_ready_replicas is None or number_of_ready_replicas < number_of_replicas:
                logging.info(f"{pod_controller} {controller_kind} does not have {number_of_replicas}/{number_of_replicas} replicas ready (only {number_of_ready_replicas or 0}/{number_of_replicas} ready). Will snooze for {snooze_seconds} seconds and try again...")
                time.sleep(snooze_seconds)
              elif number_of_ready_replicas >= number_of_replicas:
                logging.info(f"{pod_controller} {controller_kind} has {number_of_ready_replicas}/{number_of_replicas} replicas ready. Moving along...")
                all_replicas_not_available = False
                set_counter_list.append(pod_controller)
              else:
                logging.info(f"{pod_controller} {controller_kind} does not have {number_of_replicas}/{number_of_replicas} replicas ready (only {number_of_ready_replicas}/{number_of_replicas} ready). Will snooze for {snooze_seconds} seconds and try again...")
                time.sleep(snooze_seconds)
            except ApiException as e:
              logging.error("Exception when calling Kubernetes AppsV1Api: %s\n" % e)

      logging.info("100% replicas available for each ReplicaSet/StatefulSet. Moving along...")

      # Terminate the old node
      logging.info("Terminating old instance...")
      terminate_instance = ec2_client.terminate_instances(InstanceIds=[instance_id])

      # Scale ASG DesiredCapacity and MaxSize back down to original values so we are consistent with Terraform
      logging.info(f"Reverting ASG: {asg_name} with original DesiredCapacity: {current_desired_capacity} and MaxSize: {current_max_size}")
      update_asg = asg_client.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=current_desired_capacity, MaxSize=current_max_size)

    except (KeyboardInterrupt, TypeError) as e:
      errored = True
      logging.error(e)
      logging.error(f"Signal/TypeError caught! Will revert ASG: {asg_name} back to original DesiredCapacity: {current_desired_capacity} and MaxSize: {current_max_size} before exiting!")
      stream_while_running("/", ['kubectl', 'uncordon', node_name])
      update_asg = asg_client.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=current_desired_capacity, MaxSize=current_max_size)
      logging.error(f"ASG reverted back to original values. Instance {instance_id} has been uncordoned. Check state of Kubernetes and AWS before running the script again. Exiting!")
      try:
        sys.exit(0)
      except SystemExit:
        os._exit(0)

    except:
      errored = True
      logging.error("Unexpected error:", sys.exc_info()[0])
      logging.error(f"Will revert ASG: {asg_name} back to original DesiredCapacity: {current_desired_capacity} and MaxSize: {current_max_size} before exiting!")
      stream_while_running("/", ['kubectl', 'uncordon', node_name])
      update_asg = asg_client.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=current_desired_capacity, MaxSize=current_max_size)
      logging.error(f"ASG reverted back to original values. Instance {instance_id} has been uncordoned. Check state of Kubernetes and AWS before running the script again. Exiting!")
      try:
        sys.exit(0)
      except SystemExit:
        os._exit(0)
      raise

    try:
      if errored:
        logging.error(f"An error occurred above, and the ASG: {asg_name} has already been reverted to original DesiredCapacity: {current_desired_capacity} and MaxSize: {current_max_size}. Exiting!")
      else:
        # Wait for number of instances in ASG to scale back down
        logging.info("Waiting for number of instances in ASG to scale back down...")
        asg_instances_not_scaled_down = True
        while asg_instances_not_scaled_down:
          get_asg = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
          instances = get_asg['AutoScalingGroups'][0]['Instances']
          instances_lifecycle_states = [instance['LifecycleState'] for instance in instances]
          number_of_in_service_instances = instances_lifecycle_states.count("InService")
          if number_of_in_service_instances == current_desired_capacity:
            logging.info(f"Number of instances in ASG has scaled back down to original DesiredCapacity: {current_desired_capacity}")
            print("--------------------------------------------------------------------------------------------")
            print("--------------------------------------------------------------------------------------------")
            asg_instances_not_scaled_down = False
          else:
            logging.info(f"Still waiting for number of instances in ASG to scale back down. Will snooze for {snooze_seconds} seconds and try again...")
            time.sleep(snooze_seconds)
    except Exception as e:
      logging.error(e)
      logging.error(f"An error occurred above, and the infrastructure is possibily in an unstable state now. Check state of Kubernetes and AWS before running the script again. Exiting!")


if __name__ == '__main__':
  region = argv[1]
  target = argv[2]
  main(region, target)
