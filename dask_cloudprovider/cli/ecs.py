import logging
from asyncio import sleep
import sys

import click
from distributed.cli.utils import install_signal_handlers
from distributed.core import Status
from tornado.ioloop import IOLoop, TimeoutError

from dask_cloudprovider.aws import ECSCluster

logger = logging.getLogger(__name__)


@click.command()
@click.option("--debug", is_flag=True, help="Enable debug output (default off)")
@click.option("--fargate", is_flag=True, help="Turn on fargate mode (default off)")
@click.option(
    "--fargate-scheduler",
    is_flag=True,
    help="Turn on fargate mode for scheduler (default off)",
)
@click.option(
    "--fargate-workers",
    is_flag=True,
    help="Turn on fargate mode for workers (default off)",
)
@click.option(
    "--image",
    type=str,
    default=None,
    help="Docker image to use for scheduler and workers",
)
@click.option(
    "--scheduler-cpu",
    type=int,
    default=None,
    help="Scheduler CPU reservation in milli-CPU",
)
@click.option(
    "--scheduler-mem", type=int, default=None, help="Scheduler memory reservation in MB"
)
@click.option(
    "--scheduler-port",
    type=int,
    default=8786,
    help="The port on which the scheduler will be reachable to the workers and clients",
)
@click.option(
    "--scheduler-timeout",
    type=int,
    default=None,
    help="Scheduler timeout (e.g 5 minutes)",
)
@click.option(
    "--scheduler-task-arn",
    type=str,
    default=None,
    help="Scheduler task ARN (for an existing cluster, you must also specify --cluster-arn)",
)
@click.option(
    "--worker-cpu", type=int, default=None, help="Worker CPU reservation in milli-CPU"
)
@click.option(
    "--worker-mem", type=int, default=None, help="Worker memory reservation in MB"
)
@click.option(
    "--n-workers",
    type=int,
    default=None,
    help="Number of workers to start with the cluster",
)
@click.option(
    "--worker-task-arn",
    type=str,
    default=None,
    metavar='ARN',
    help="Worker task ARN (for an existing cluster, you must also specify --cluster-arn)",
)
@click.option(
    "--cluster-arn",
    type=str,
    default=None,
    metavar='ARN',
    help="The ARN of an existing ECS cluster to use",
)
@click.option(
    "--cluster-name-template",
    type=str,
    default=None,
    help="A template to use for the cluster name if `--cluster-arn` is not set",
)
@click.option(
    "--execution-role-arn",
    type=str,
    default=None,
    metavar='ARN',
    help="The ARN of an existing IAM role to use for ECS execution",
)
@click.option(
    "--task-role-arn",
    type=str,
    default=None,
    metavar='ARN',
    help="The ARN of an existing IAM role to give to the tasks",
)
@click.option(
    "--task-role-policy",
    type=str,
    default=None,
    multiple=True,
    help="Policy to attach to a task if --task-role-arn is not set (can be used multiple times)",
)
@click.option(
    "--cloudwatch-logs-group", type=str, default=None, help="The group to send logs to"
)
@click.option(
    "--cloudwatch-logs-stream-prefix",
    type=str,
    default=None,
    help="An optional prefix to use for log streams",
)
@click.option(
    "--cloudwatch-logs-default-retention",
    type=int,
    default=None,
    help="Number of says to retain logs",
)
@click.option(
    "--vpc",
    type=str,
    default=None,
    metavar='<vpc-id>',
    help="The ID of an existing VPC (defaults to 'default' VPC). E.g.: vpc-1234567890abcdef0",
)
@click.option(
    "--subnet",
    type=str,
    default=None,
    multiple=True,
    metavar='<subnet-id>',
    help="VPC subnet to use (defaults to all subnets, E.g.: subnet-0397b6c47c42e4dc0)",
)
@click.option(
    "--security-group",
    type=str,
    default=None,
    multiple=True,
    help="Security group to use for task communication (can be used multiple times, will be created if not specified)",
)
@click.option(
    "--environment",
    type=str,
    default=None,
    multiple=True,
    help="Environment variable for the scheduler and workers in the form FOO=bar (can be used multiple times)",
)
@click.option(
    "--tag",
    type=str,
    default=None,
    multiple=True,
    help="Tag to apply to all resources created automatically in the form FOO=bar (can be used multiple times)",
)
@click.option("--skip_cleanup", is_flag=True, help="Skip cleanup of stale resources")
@click.version_option()
def main(debug, **kwargs):
    # Each click option adds a variable to the parameters of this function that
    # corresponds to the option name, but all '-' characters are replaced with
    # '_'.
    #
    # Some CLI options map to arrays, where multiple declarations of the option
    # are gathered into lists prior to passing to `ECSCluster()`.
    #
    kwargs['tags'] = {v.split("=")[0]: v.split("=")[1] for v in kwargs['tag']} if kwargs['tag'] else None
    del kwargs['tag']

    kwargs['environment'] = (
        { v.split("=")[0]: v.split("=")[1] for v in kwargs['environment'] } if kwargs['environment'] else None
    )

    kwargs['subnets'] = kwargs['subnet'] or None
    del kwargs['subnet']

    kwargs['security_groups'] = kwargs['security_group'] or None
    del kwargs['security_group']

    kwargs['task_role_policies'] = kwargs['task_role_policy'] or None
    del kwargs['task_role_policy']

    kwargs.update({
        'fargate_scheduler': kwargs['fargate_scheduler'] or kwargs['fargate'],
        'fargate_workers': kwargs['fargate_workers'] or kwargs['fargate']
    })

    if kwargs['scheduler_task_arn'] != None:
        if not kwargs['cluster_arn']:
            raise click.UsageError("--cluster-arn must be provided when using --scheduler-task-arn")
        kwargs.update({ 'scheduler_task_definition_arn': kwargs['scheduler_task_arn'] })

    if kwargs['worker_task_arn'] != None:
        if not kwargs['cluster_arn']:
            raise click.UsageError("--cluster-arn must be provided when using --scheduler-task-arn")
        kwargs.update({ 'worker_task_definition_arn': kwargs['worker_task_arn'] })

    # Clean up remaining keyword arguments to `main()` that do not correspond to
    # a constructor argument for `ECSCluster()`.
    del kwargs['fargate']
    del kwargs['scheduler_task_arn']
    del kwargs['worker_task_arn']

    logger.info("Starting ECS cluster")
    try:
        cluster = ECSCluster(**kwargs)
    except click.ClickException as e:
        logger.error(str(e) + "\n")
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    except Exception as e:
        if debug:
            traceback.print_exc()
        logger.error(str(e) + "\n")
        sys.exit(1)

    async def run():
        logger.info("Ready")
        while cluster.status != Status.closed:
            await sleep(0.2)

    def on_signal(signum):
        logger.info("Exiting on signal %d", signum)
        cluster.close(timeout=2)

    loop = IOLoop.current()
    install_signal_handlers(loop, cleanup=on_signal)

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        logger.info("Shutting down")
    finally:
        logger.info("End dask-ecs")


if __name__ == "__main__":
    main()
