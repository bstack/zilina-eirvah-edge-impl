# Local study fixture; no packages added or existing simulator image overwritten.
FROM modbus-simulator:local
COPY services/modbus-simulator/src /workspace/services/modbus-simulator/src
