# Teltonika Configuration
## Modbus TCP to REST
Configuration how to set up the Teltonika router to query register with Modbus TCP and sending the contents of the 
registers to a REST server.

### WebUI Configuration
- Navigate: `Services / Data to Server -> Add new instance`
- Name: `<name>` (use any name you want)
- Click: `Add`

Data configuration
- Name: `Modbus`
- Type: `Modbus`
- Format type: `Custom`
- Format string: `{"timestamp": %timestamp%, "name": "%name%", "server_name": "%server_name%", "data": [%data%]}`
- Empty value: `null`
- Delimiter: `,`
- Data filtering: `All`
- Segment count: `1`
- Send as object: `off`
- Click: `Next: Collection edit`

Collection configuration
- Enable: `on`
- Format type: `Custom`
- Format string: `{"modbus": %Modbus%}`
- Empty value: `null`
- Period: `60`
- Click: `Next: Server configuration`

Server configuration
- Type: `HTTP`
- Server address: `x.x.x.x`
- HTTP headers: `Authorization: Basic <base64(<username>:<password>)>`  
  E.g. `username=frodo` and `password=schatz`, then the Base64 encoding can be calculated in Python with
  ```python
  import base64
  
  username = b"frodo"
  password = b"schatz"
  
  encoded = base64.b64encode(username + b":" + password)
  # b'ZnJvZG86c2NoYXR6'
  ```
  and the header is `Authorization: Basic ZnJvZG86c2NoYXR6`
- Enable: secure connection `off`
- Click: `Save & Apply`

# Janiza UMG96 Registers
- Voltage line to line ([0]: U_L1L2, [1]: U_L2L3, [2]: U_L3L1)
    - Data type: `32 bit float Byte order 1,2,3,4`
    - Function: `Read holding registers`
    - First register number: `19007`
    - Register count / Values: `6`
- Power ([0]: U_L1L2, [1]: U_L2L3, [2]: U_L3L1)
    - Data type: `32 bit float Byte order 1,2,3,4`
    - Function: `Read holding registers`
    - First register number: `19007`
    - Register count / Values: `6`
- Frequency ([0]: Frequency)
    - Data type: `32 bit float Byte order 1,2,3,4`
    - Function: `Read holding registers`
    - First register number: `19051`
    - Register count / Values: `2`
