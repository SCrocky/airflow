# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from airflow.providers.common.compat.sdk import BaseOperatorLink
from airflow.providers.http.hooks import HttpHook


class ExternalHttpDagLink(BaseOperatorLink):
    """
    Operator link for ExternalHttpTaskSensor.

    It allows users to access DAG waited with ExternalHttpTaskSensor.
    """

    name = "External DAG"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        from urllib.parse import urljoin

        if TYPE_CHECKING:
            assert isinstance(operator, (ExternalTaskMarker, ExternalTaskSensor))

        external_dag_id = operator.external_dag_id
        external_url = operator.external_url

        return urljoin(external_url.rstrip("/") + "/", f"dags/{external_dag_id}/runs/{ti_key.run_id}")

try:
    from airflow.providers.standard import ExternalTaskSensor as _ExternalTaskSensor
except ImportError:
    class ExternalHttpTaskSensor:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("Requires Airflow Standard Providers to work")
else:
    class ExternalHttpTaskSensor(_ExternalTaskSensor):
        ...


class ExternalTaskSensor(BaseSensorOperator):
    """
    Waits for a different DAG, task group, or task to complete for a specific logical date.

    If both `external_task_group_id` and `external_task_id` are ``None`` (default), the sensor
    waits for the DAG.
    Values for `external_task_group_id` and `external_task_id` can't be set at the same time.

    By default, the ExternalTaskSensor will wait for the external task to
    succeed, at which point it will also succeed. However, by default it will
    *not* fail if the external task fails, but will continue to check the status
    until the sensor times out (thus giving you time to retry the external task
    without also having to clear the sensor).

    By default, the ExternalTaskSensor will not skip if the external task skips.
    To change this, simply set ``skipped_states=[TaskInstanceState.SKIPPED]``.
    Note that if you are monitoring multiple tasks, and one enters error state
    and the other enters a skipped state, then the external task will react to
    whichever one it sees first. If both happen together, then the failed state
    takes priority.

    It is possible to alter the default behavior by setting states which
    cause the sensor to fail, e.g. by setting ``allowed_states=[DagRunState.FAILED]``
    and ``failed_states=[DagRunState.SUCCESS]`` you will flip the behaviour to
    get a sensor which goes green when the external task *fails* and immediately
    goes red if the external task *succeeds*!

    Note that ``soft_fail`` is respected when examining the failed_states. Thus
    if the external task enters a failed state and ``soft_fail == True`` the
    sensor will _skip_ rather than fail. As a result, setting ``soft_fail=True``
    and ``failed_states=[DagRunState.SKIPPED]`` will result in the sensor
    skipping if the external task skips. However, this is a contrived
    example---consider using ``skipped_states`` if you would like this
    behaviour. Using ``skipped_states`` allows the sensor to skip if the target
    fails, but still enter failed state on timeout. Using ``soft_fail == True``
    as above will cause the sensor to skip if the target fails, but also if it
    times out.
    :param http_conn_id: :ref:`http connection<howto/connection:http>` that has the base
        API url i.e https://www.my_airflow.com and optional authentication credentials.
        Default headers can also be specified in the Extra field in json format.
    :param external_dag_id: The dag_id that contains the task you want to
        wait for. (templated)
    :param external_task_id: The task_id that contains the task you want to
        wait for. (templated)
    :param external_task_ids: The list of task_ids that you want to wait for. (templated)
        If ``None`` (default value) the sensor waits for the DAG. Either
        external_task_id or external_task_ids can be passed to
        ExternalTaskSensor, but not both.
    :param external_task_group_id: The task_group_id that contains the task you want to
        wait for. (templated)
    :param allowed_states: Iterable of allowed states, default is ``['success']``
    :param skipped_states: Iterable of states to make this task mark as skipped, default is ``None``
    :param failed_states: Iterable of failed or dis-allowed states, default is ``None``
    :param execution_delta: time difference with the previous execution to
        look at, the default is the same logical date as the current task or DAG.
        For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to
        ExternalTaskSensor, but not both.
    :param execution_date_fn: function that receives the current execution's logical date as the first
        positional argument and optionally any number of keyword arguments available in the
        context dictionary, and returns the desired logical dates to query.
        Either execution_delta or execution_date_fn can be passed to ExternalTaskSensor,
        but not both.
    :param check_existence: Set to `True` to check if the external task exists (when
        external_task_id is not None) or check if the DAG to wait for exists (when
        external_task_id is None), and immediately cease waiting if the external task
        or DAG does not exist (default value: False).
    :param poll_interval: polling period in seconds to check for the status
    :param deferrable: Run sensor in deferrable mode
    """

    template_fields = ["external_dag_id", "external_task_id", "external_task_ids", "external_task_group_id"]
    ui_color = "#4db7db"
    operator_extra_links = [ExternalHttpDagLink()]

    def __init__(
        self,
        *,
        external_conn_id: str,
        external_dag_id: str,
        external_task_id: str | None = None,
        external_task_ids: Collection[str] | None = None,
        external_task_group_id: str | None = None,
        allowed_states: Iterable[str] | None = None,
        skipped_states: Iterable[str] | None = None,
        failed_states: Iterable[str] | None = None,
        execution_delta: datetime.timedelta | None = None,
        execution_date_fn: Callable | None = None,
        check_existence: bool = False,
        poll_interval: float = 2.0,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.external_conn_id = external_conn_id

    def _get_dttm_filter(self, context: Context) -> Sequence[datetime.datetime]:
        logical_date = self._get_logical_date(context)

        if self.execution_delta:
            return [logical_date - self.execution_delta]
        if self.execution_date_fn:
            result = self._handle_execution_date_fn(context=context)
            return result if isinstance(result, list) else [result]
        return [logical_date]

    @staticmethod
    def _serialize_dttm_filter(dttm_filter: Sequence[datetime.datetime]) -> str:
        return ",".join(dt.isoformat() for dt in dttm_filter)

    def check_for_task():
        hook = HttpHook(self.external_conn_id)
        endpoint = f"/api/v2/dags/{self.external_dag_id}/dagRuns/~/taskInstances"
        data={
                "task_id": task_id,
        }
        if self.external_task_group_id:
            data |= {"task_group_id": self.external_task_group_id}

        hook.run(
            endpoint=endpoint,
            data=data,
        )

    def poke(self, context: Context) -> bool:
        # delay check to poke rather than __init__ in case it was supplied as XComArgs
        if self.external_task_ids and len(self.external_task_ids) > len(set(self.external_task_ids)):
            raise ValueError("Duplicate task_ids passed in external_task_ids parameter")

        dttm_filter = self._get_dttm_filter(context)
        serialized_dttm_filter = self._serialize_dttm_filter(dttm_filter)
        # Save as attribute - to be used by listeners
        self.external_dates_filter = serialized_dttm_filter

        if self.external_task_ids:
            self.log.info(
                "Poking for tasks %s in dag %s on %s ... ",
                self.external_task_ids,
                self.external_dag_id,
                serialized_dttm_filter,
            )

        if self.external_task_group_id:
            self.log.info(
                "Poking for task_group '%s' in dag '%s' on %s ... ",
                self.external_task_group_id,
                self.external_dag_id,
                serialized_dttm_filter,
            )

        if self.external_dag_id and not self.external_task_group_id and not self.external_task_ids:
            self.log.info(
                "Poking for DAG '%s' on %s ... ",
                self.external_dag_id,
                serialized_dttm_filter,
            )

        if AIRFLOW_V_3_0_PLUS:
            return self._poke_af3(context, dttm_filter)
        return self._poke_af2(dttm_filter)

    def _poke_af3(self, context: Context, dttm_filter: Sequence[datetime.datetime]) -> bool:
        from airflow.providers.standard.utils.sensor_helper import _get_count_by_matched_states

        self._has_checked_existence = True
        ti = context["ti"]

        def _get_count(states: list[str]) -> int:
            if self.external_task_ids:
                return ti.get_ti_count(
                    dag_id=self.external_dag_id,
                    task_ids=list(self.external_task_ids),
                    logical_dates=list(dttm_filter),
                    states=states,
                )
            if self.external_task_group_id:
                run_id_task_state_map = ti.get_task_states(
                    dag_id=self.external_dag_id,
                    task_group_id=self.external_task_group_id,
                    logical_dates=list(dttm_filter),
                )
                return _get_count_by_matched_states(run_id_task_state_map, states)
            return ti.get_dr_count(
                dag_id=self.external_dag_id,
                logical_dates=list(dttm_filter),
                states=states,
            )

        if self.failed_states:
            count = _get_count(self.failed_states)
            count_failed = self._calculate_count(count, dttm_filter)
            self._handle_failed_states(count_failed)

        if self.skipped_states:
            count = _get_count(self.skipped_states)
            count_skipped = self._calculate_count(count, dttm_filter)
            self._handle_skipped_states(count_skipped)

        count = _get_count(self.allowed_states)
        count_allowed = self._calculate_count(count, dttm_filter)
        return count_allowed == len(dttm_filter)

    def _calculate_count(self, count: int, dttm_filter: Sequence[datetime.datetime]) -> float | int:
        """Calculate the normalized count based on the type of check."""
        if self.external_task_ids:
            return count / len(self.external_task_ids)
        return count

    def _handle_failed_states(self, count_failed: float | int) -> None:
        """Handle failed states and raise appropriate exceptions."""
        if count_failed > 0:
            if self.external_task_ids:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f"Some of the external tasks {self.external_task_ids} "
                        f"in DAG {self.external_dag_id} failed. Skipping due to soft_fail."
                    )
                raise ExternalTaskFailedError(
                    f"Some of the external tasks {self.external_task_ids} "
                    f"in DAG {self.external_dag_id} failed."
                )
            if self.external_task_group_id:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f"The external task_group '{self.external_task_group_id}' "
                        f"in DAG '{self.external_dag_id}' failed. Skipping due to soft_fail."
                    )
                raise ExternalTaskGroupFailedError(
                    f"The external task_group '{self.external_task_group_id}' "
                    f"in DAG '{self.external_dag_id}' failed."
                )
            if self.soft_fail:
                raise AirflowSkipException(
                    f"The external DAG {self.external_dag_id} failed. Skipping due to soft_fail."
                )
            raise ExternalDagFailedError(f"The external DAG {self.external_dag_id} failed.")

    def _handle_skipped_states(self, count_skipped: float | int) -> None:
        """Handle skipped states and raise appropriate exceptions."""
        if count_skipped > 0:
            if self.external_task_ids:
                raise AirflowSkipException(
                    f"Some of the external tasks {self.external_task_ids} "
                    f"in DAG {self.external_dag_id} reached a state in our states-to-skip-on list. Skipping."
                )
            if self.external_task_group_id:
                raise AirflowSkipException(
                    f"The external task_group '{self.external_task_group_id}' "
                    f"in DAG {self.external_dag_id} reached a state in our states-to-skip-on list. Skipping."
                )
            raise AirflowSkipException(
                f"The external DAG {self.external_dag_id} reached a state in our states-to-skip-on list. "
                "Skipping."
            )

    if not AIRFLOW_V_3_0_PLUS:

        @provide_session
        def _poke_af2(self, dttm_filter: Sequence[datetime.datetime], session: Session = NEW_SESSION) -> bool:
            if self.check_existence and not self._has_checked_existence:
                self._check_for_existence(session=session)

            if self.failed_states:
                count_failed = self.get_count(dttm_filter, session, self.failed_states)
                self._handle_failed_states(count_failed)

            if self.skipped_states:
                count_skipped = self.get_count(dttm_filter, session, self.skipped_states)
                self._handle_skipped_states(count_skipped)

            count_allowed = self.get_count(dttm_filter, session, self.allowed_states)
            return count_allowed == len(dttm_filter)

    def execute(self, context: Context) -> None:
        """Run on the worker and defer using the triggers if deferrable is set to True."""
        if not self.deferrable:
            super().execute(context)
        else:
            dttm_filter = self._get_dttm_filter(context)
            if AIRFLOW_V_3_0_PLUS:
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=WorkflowTrigger(
                        external_dag_id=self.external_dag_id,
                        external_task_group_id=self.external_task_group_id,
                        external_task_ids=self.external_task_ids,
                        allowed_states=self.allowed_states,
                        failed_states=self.failed_states,
                        skipped_states=self.skipped_states,
                        poke_interval=self.poll_interval,
                        soft_fail=self.soft_fail,
                        logical_dates=list(dttm_filter),
                        run_ids=None,
                        execution_dates=None,
                    ),
                    method_name="execute_complete",
                )
            else:
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=WorkflowTrigger(
                        external_dag_id=self.external_dag_id,
                        external_task_group_id=self.external_task_group_id,
                        external_task_ids=self.external_task_ids,
                        allowed_states=self.allowed_states,
                        failed_states=self.failed_states,
                        skipped_states=self.skipped_states,
                        poke_interval=self.poll_interval,
                        soft_fail=self.soft_fail,
                        execution_dates=list(dttm_filter),
                        logical_dates=None,
                        run_ids=None,
                    ),
                    method_name="execute_complete",
                )

    def execute_complete(self, context: Context, event: dict[str, typing.Any] | None = None) -> None:
        """Execute when the trigger fires - return immediately."""
        if event is None:
            raise ExternalTaskNotFoundError("No event received from trigger")

        # Re-set as attribute after coming back from deferral - to be used by listeners
        self.external_dates_filter = self._serialize_dttm_filter(self._get_dttm_filter(context))

        if event["status"] == "success":
            self.log.info("External tasks %s has executed successfully.", self.external_task_ids)
        elif event["status"] == "skipped":
            raise AirflowSkipException("External job has skipped skipping.")
        elif event["status"] == "failed":
            if self.soft_fail:
                raise AirflowSkipException("External job has failed skipping.")
            raise ExternalDagFailedError("External job has failed.")
        else:
            if self.soft_fail:
                raise AirflowSkipException("External job has failed skipping.")
            raise ExternalTaskNotFoundError(
                "Error occurred while trying to retrieve task status. Please, check the "
                "name of executed task and Dag."
            )

    def _check_for_existence(self, session: Session) -> None:
        dag_to_wait = DagModel.get_current(self.external_dag_id, session)

        if not dag_to_wait:
            raise ExternalDagNotFoundError(f"The external DAG {self.external_dag_id} does not exist.")

        path = correct_maybe_zipped(dag_to_wait.fileloc)
        if not path or not os.path.exists(path):
            raise ExternalDagDeletedError(f"The external DAG {self.external_dag_id} was deleted.")

        if self.external_task_ids:
            refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(self.external_dag_id)
            if not refreshed_dag_info:
                raise ExternalDagNotFoundError(
                    f"The external DAG {self.external_dag_id} could not be loaded."
                )
            for external_task_id in self.external_task_ids:
                if not refreshed_dag_info.has_task(external_task_id):
                    raise ExternalTaskNotFoundError(
                        f"The external task {external_task_id} in DAG {self.external_dag_id} does not exist."
                    )

        if self.external_task_group_id:
            refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(self.external_dag_id)
            if not refreshed_dag_info:
                raise ExternalDagNotFoundError(
                    f"The external DAG {self.external_dag_id} could not be loaded."
                )
            if not refreshed_dag_info.has_task_group(self.external_task_group_id):
                raise ExternalTaskGroupNotFoundError(
                    f"The external task group '{self.external_task_group_id}' in "
                    f"DAG '{self.external_dag_id}' does not exist."
                )

        self._has_checked_existence = True

    def get_count(self, dttm_filter: Sequence[datetime.datetime], session: Session, states: list[str]) -> int:
        """
        Get the count of records against dttm filter and states.

        :param dttm_filter: date time filter for logical date
        :param session: airflow session object
        :param states: task or dag states
        :return: count of record against the filters
        """
        warnings.warn(
            "This method is deprecated and will be removed in future.", DeprecationWarning, stacklevel=2
        )
        return _get_count(
            dttm_filter,
            self.external_task_ids,
            self.external_task_group_id,
            self.external_dag_id,
            states,
            session,
        )

    def get_external_task_group_task_ids(
        self, session: Session, dttm_filter: Sequence[datetime.datetime]
    ) -> list[tuple[str, int]]:
        warnings.warn(
            "This method is deprecated and will be removed in future.", DeprecationWarning, stacklevel=2
        )
        if self.external_task_group_id is None:
            return []
        return _get_external_task_group_task_ids(
            list(dttm_filter), self.external_task_group_id, self.external_dag_id, session
        )

    def _get_logical_date(self, context: Context) -> datetime.datetime:
        """
        Handle backwards- and forwards-compatible retrieval of the date.

        to pass as the positional argument to execution_date_fn.
        """
        # Airflow 3.x: contexts define "logical_date" (or fall back to dag_run.run_after).
        if AIRFLOW_V_3_0_PLUS:
            logical_date = context.get("logical_date")
            dag_run = context.get("dag_run")
            if logical_date:
                return logical_date
            if dag_run and hasattr(dag_run, "run_after") and dag_run.run_after:
                return dag_run.run_after
            raise ValueError("Either `logical_date` or `dag_run.run_after` must be provided in the context")

        # Airflow 2.x and earlier: contexts used "execution_date"
        execution_date = context.get("execution_date")
        if not execution_date:
            raise ValueError("Either `execution_date` must be provided in the context`")
        if not isinstance(execution_date, datetime.datetime):
            raise ValueError("execution_date must be a datetime object")
        return execution_date

    def _handle_execution_date_fn(self, context: Context) -> datetime.datetime | list[datetime.datetime]:
        """
        Handle backward compatibility.

        This function is to handle backwards compatibility with how this operator was
        previously where it only passes the logical date, but also allow for the newer
        implementation to pass all context variables as keyword arguments, to allow
        for more sophisticated returns of dates to return.
        """
        from airflow.utils.operator_helpers import make_kwargs_callable

        # Remove "logical_date" because it is already a mandatory positional argument
        logical_date = self._get_logical_date(context)
        kwargs = {k: v for k, v in context.items() if k not in {"execution_date", "logical_date"}}
        # Add "context" in the kwargs for backward compatibility (because context used to be
        # an acceptable argument of execution_date_fn)
        kwargs["context"] = context
        if TYPE_CHECKING:
            assert self.execution_date_fn is not None
        kwargs_callable = make_kwargs_callable(self.execution_date_fn)
        return kwargs_callable(logical_date, **kwargs)

