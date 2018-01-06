# Connector Module

Each connector functions as a pre-formatter. It pre-processes the data into a tuple of the following
layout:

```
(event, seq_no, data)
```

- Whereas `event` is the channel name this data came on.
- `seq_no` is the sequence number as given by the server to this message. If the API does not
provide sequence numbers, this is replaced (in order of priority): API_timestamp as given by the
server, Timestamp, as given by the CTS and finally `None`.
 - `data` contains the *raw* message. It should not be modified, as the `Node` processing this
 data is likely going to send it out raw, in addition to sending it out formatted.
