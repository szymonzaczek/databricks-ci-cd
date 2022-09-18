import os
import json
import re
import datetime

from read_config import read_env_cfg
from databricks_api_class_internal import DatabricksRequest
from databricks_api_workflows_internal import read_token_from_file

if os.environ.get("ENVIRONMENT_NAME") == "prd_bi":
    ENVIRONMENT_NAME = "prd"
else:
    ENVIRONMENT_NAME = os.environ.get("ENVIRONMENT_NAME")
print(f"Environment name: {ENVIRONMENT_NAME}")


def databricks_jobs_ci(
    cfg_path: str,
    secret_path: str,
    jobs_to_deploy: str,
    output_file_path: str,
    jobs_suffix: str = "_deployed",
    target_path: str = "/bi_prd",
    time_offset: str = "+00:30:00",
    jobs_name_replace_regex: str = "(pre[-_\s]*)",
) -> None:
    """CI step for BI Workflow redeployment of jobs.
    This step will make necessery changes in job settings and save them in json file that will be consumed by CD step.

    :param cfg_path: path to the configuration
    :type cfg_path: str
    :param secret_path: path to the secret file
    :type secret_path: str
    :param jobs_to_deploy: id of the jobs that are to be redeployed
    :type jobs_to_deploy: str
    :param output_file_path: path where job settings will be saved
    :type output_file_path: str
    :param jobs_suffix: suffix added to the redeployed job name
    :type jobs_suffix: str
    :param target_path: target path of the notebooks
    :type target_path: str
    :param time_offset: amount of time added to the time of the original job deplyment, "%H:%M:%S" format
    :type time_offset: str = "00:30:00"
    :param jobs_name_replace_regex: regex used for clearing job name
    :type jobs_name_replace_regex: str
    :return: None
    :rtype: None
    """
    print(f"Extracting jobs from Databricks workspace.")
    print(f"ENVIRONMENT_NAME: {ENVIRONMENT_NAME}")
    print(f"jobs to deploy: {jobs_to_deploy}; typ: {type(jobs_to_deploy)}; ")
    print(f"eval: {eval(jobs_to_deploy)}; eval type: {type(eval(jobs_to_deploy))}")

    jobs_to_deploy = eval(jobs_to_deploy)
    if len(jobs_to_deploy) == 0:
        return None
    print(f"Jobs to deploy after eval: {jobs_to_deploy}; type: {type(jobs_to_deploy)}")

    # create API object
    databricks_token = read_token_from_file(secret_path)
    cfg = read_env_cfg(ENVIRONMENT_NAME, cfg_path)
    api_object = DatabricksRequest(cfg.get("databricks_host"), None, databricks_token)

    # get current jobs in workspace
    jobs_list_raw = api_object.get_jobs_list()
    print(f"Raw Jobs list: {jobs_list_raw}")
    print(f'Job IDs: {[job["job_id"] for job in jobs_list_raw.get("jobs")]}')

    changed_jobs_settings = []
    for job_id in jobs_to_deploy:
        print(f"Job id: {job_id} type: {type(job_id)}")
        if job_id not in [str(x["job_id"]) for x in jobs_list_raw.get("jobs")]:
            print(
                f"Job ID specified in json file: {job_id} is not implemented on "
                f"Databricks"
            )
        else:
            print(f"There is a job with id {job_id} implemented on Databricks")
            print(f"Proceed with deployment")
            job_info = api_object.get_job_info(str(job_id))
            job_settings = job_info.get("settings")
            print(f"job_info: {job_info}")
            print(f"Job settings: {job_settings}")

            # move schedule of this job by time_offset
            scheduled_time = (
                job_settings.get("schedule").get("quartz_cron_expression").split(" ")
            )
            scheduled_time_string = scheduled_time[2] + ":" + scheduled_time[1]
            scheduled_time_object = datetime.datetime.strptime(
                scheduled_time_string, "%H:%M"
            )
            time_offset_object = datetime.datetime.strptime(time_offset[1:], "%H:%M:%S")
            delta = datetime.timedelta(
                hours=time_offset_object.hour,
                minutes=time_offset_object.minute,
                seconds=time_offset_object.second,
            )
            if time_offset[0] == "-":
                delta = -delta
            target_schedule = scheduled_time_object + delta
            scheduled_time[2] = str(target_schedule.hour)
            scheduled_time[1] = str(target_schedule.minute)
            job_settings["schedule"]["quartz_cron_expression"] = " ".join(
                scheduled_time
            )

            for i in range(len(job_settings["tasks"])):
                job_settings["tasks"][i]["notebook_task"]["notebook_path"] = (
                    target_path
                    + "/"
                    + "/".join(
                        job_settings["tasks"][i]["notebook_task"][
                            "notebook_path"
                        ].split("/")[-2:]
                    )
                )
            job_settings["name"] = re.sub(
                jobs_name_replace_regex, "", job_settings["name"]
            )
            job_settings["name"] = job_settings["name"] + jobs_suffix
            print(
                f"Job settings after giving permissions, renaming and replacing preprd"
                f' paths: {job_info.get("settings")}'
            )

            changed_jobs_settings.append(job_settings)

    print(f"{output_file_path}/{ENVIRONMENT_NAME}_jobs.json")

    with open(f"{output_file_path}/{ENVIRONMENT_NAME}_jobs.json", "w") as f:
        f.write(json.dumps(changed_jobs_settings, indent=2))


def databricks_jobs_cd(
    cfg_path: str,
    secret_path: str,
    jobs_to_deploy_path: str,
    group_manage_permissions: str,
    jobs_cluster_id: str,
) -> None:
    """CD step for redeployment of BI jobs.
    This step will consume prepered jobs in CI step and upload them with correct settings to target environment.

    :param cfg_path: path to the configuration
    :type cfg_path: str
    :param secret_path: path to the secret file
    :type secret_path: str
    :param jobs_to_deploy: path to json file with jobs settings
    :type jobs_to_deploy: str
    :param group_manage_permissions: name of the group on Databricks workspace that is the owner of the jobs
    :type group_manage_permissions: str
    :param jobs_cluster_id: ID of BI job cluster on target environment
    :type jobs_cluster_id: str

    :return: None
    :rtype: None
    """
    print(f"Jobs to deploy path: {jobs_to_deploy_path}")

    if not os.path.exists(jobs_to_deploy_path):
        print(f"No jobs for file: {jobs_to_deploy_path}")
        return

    with open(jobs_to_deploy_path, "r") as f:
        jobs_to_deploy = json.load(f)

    databricks_token = read_token_from_file(secret_path)
    cfg = read_env_cfg(ENVIRONMENT_NAME, cfg_path)
    api_object = DatabricksRequest(cfg.get("databricks_host"), None, databricks_token)

    # get currently existing jobs
    jobs_list_raw = api_object.get_jobs_list()
    job_list = jobs_list_raw.get("jobs")
    if job_list is None:
        job_list = []
    print(f"Job list: {job_list}")
    job_names = [x["settings"]["name"] for x in job_list]
    print(f"Job names: {job_names}")

    for job_settings in jobs_to_deploy:
        print(job_settings)

        # change job cluster ID to target environment cluster ID
        for task in job_settings["tasks"]:
            task["existing_cluster_id"] = jobs_cluster_id

        if job_settings["name"] not in job_names:
            print(f"Creating new job with the name: {job_settings['name']}")
            deployed_job = api_object.create_job(job_settings)
            print(f"Deployed job: {deployed_job}")
            deployed_job_id = deployed_job.get("job_id")
        else:
            job_id_to_redeploy = [
                x["job_id"]
                for x in job_list
                if x["settings"]["name"] == job_settings["name"]
            ][0]
            print(
                f"ID of the deployed job with already existing name: {job_id_to_redeploy}"
            )
            new_job_settings = dict()
            new_job_settings["new_settings"] = job_settings
            new_job_settings["job_id"] = str(job_id_to_redeploy)
            print(
                f"Updating job with the name: {new_job_settings['new_settings']['name']}"
            )
            print(f"Full settings of the job that is resetted: {new_job_settings}")
            deployed_job = api_object.reset_job(new_job_settings)
            print(f"Deployed job: {deployed_job}")
            deployed_job_id = new_job_settings.get("job_id")

        print(f"Getting permissions for a job")
        jobs_permissions = api_object.get_job_permission(deployed_job_id)
        print(f"Jobs permissions: {jobs_permissions}")
        if group_manage_permissions not in [
            x.get("group_name") for x in jobs_permissions.get("access_control_list")
        ]:
            print(f"There is no permissions for this group yet - please update it")
            access_control_list = [
                {
                    "group_name": group_manage_permissions,
                    "permission_level": "CAN_MANAGE",
                }
            ]
            update_permissions = api_object.update_job_permissions(
                deployed_job_id, access_control_list
            )
            print(f"Update permissions: {update_permissions}")
