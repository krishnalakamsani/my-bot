from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import logging

from execution import place_order as local_place_order

app = FastAPI(title="Execution API")
logging.basicConfig(level=logging.INFO)


class ExecRequest(BaseModel):
    security_id: str
    transaction_type: str
    qty: int
    index_name: str | None = None


@app.post("/execute")
def execute(req: ExecRequest):
    try:
        logging.info("Received execute request: %s", req.dict())
        # call execution.place_order(side, security_id, quantity, order_type, price)
        res = local_place_order(req.transaction_type, req.security_id, req.qty, price=None)
        return res or {"status": "error", "message": "unknown"}
    except Exception as e:
        logging.exception("Execution error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
