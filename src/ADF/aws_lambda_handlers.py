import json

from typing import Dict

from ADF.components.flow_config import ADFCollection
from ADF.components.implementers import AWSImplementer
from ADF.utils import s3_client


def aws_lambda_apply_step_from_sqs(event: Dict, _) -> None:
    for record in event["Records"]:
        params: Dict[str, str] = json.loads(record["body"])
        implementer = AWSImplementer.from_yaml_string(
            s3_client.get_object(
                Bucket=params["bucket"],
                Key=params["s3_icp"],
            )["Body"]
            .read()
            .decode("utf-8"),
        )
        flows = ADFCollection.from_yaml_string(
            s3_client.get_object(
                Bucket=params["bucket"],
                Key=params["s3_fcp"],
            )["Body"]
            .read()
            .decode("utf-8")
        )
        if params["is_combination_step"]:
            implementer.apply_combination_step(
                combination_step=flows.get_step(
                    params["combination_step_flow_name"],
                    params["combination_step_step_name"],
                ),
                batch_args=params["batch_args"],
                batch_id=params["batch_id"],
            )
        else:
            implementer.apply_step(
                step_in=flows.get_step(
                    params["step_in_flow_name"], params["step_in_step_name"]
                ),
                step_out=flows.get_step(
                    params["step_out_flow_name"], params["step_out_step_name"]
                ),
                batch_id=params["batch_id"],
            )
