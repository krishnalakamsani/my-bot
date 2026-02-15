import os
import logging

MAX_POSITION = int(os.getenv("MAX_POSITION", "200"))

def check_risk(current_positions, side, quantity):
    # current_positions: integer representing current net quantity
    if side == "BUY":
        projected = current_positions + quantity
    else:
        projected = current_positions - quantity
    if abs(projected) > MAX_POSITION:
        logging.warning("Risk limit exceeded: projected %s > max %s", projected, MAX_POSITION)
        return False
    return True
