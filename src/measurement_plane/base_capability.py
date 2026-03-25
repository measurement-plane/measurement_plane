from measurement_plane.messaging.message import CapabilityMessage
import queue, asyncio

class BaseCapability:
    """
    Capability Base Class
    """

    def __init__(self, name:str):
        self.name = name
        self.aliases = []
        self.agent = None
        self.endpoint = None
        self.label = None
        self.parameters_schema = None
        self.result_schema = None
        self.plot_schema = None
        self.metadata = None
        self.type = None

    def set_agent(self, agent):
        self.agent = agent
        self.endpoint = agent.endpoint

    def construct_capability(self):
        metadata = dict(self.metadata or {})
        if self.aliases:
            metadata["aliases"] = list(self.aliases)
        capability = CapabilityMessage()
        capability.construct(
            label=self.label,
            endpoint=self.endpoint,
            capability_name=self.name,
            parameters_schema=self.parameters_schema,
            result_schema=self.result_schema,
            plot_schema=self.plot_schema,
            metadata=metadata or None,
            type=self.type
        )
        self.message = capability.message
        return capability.message

    def matches_specification(self, specification_msg):
        endpoint = specification_msg.get("endpoint")
        capability_name = specification_msg.get("capabilityName")
        if endpoint != self.endpoint:
            return False

        accepted_names = {self.name, *self.aliases}
        return capability_name in accepted_names
    
    async def async_execute(self, parameters):
        """
        Override for one-time or periodic measurement
        Return a single result dictionary or None
        """
        raise NotImplementedError("async_execute() must be implemented by subclass")

    async def async_stream(self, parameters, result_queue):
        """
        Override for streaming measurements
        Must be an async generator:
            async for result in self.async_stream(parameters):
                ...
        """
        raise NotImplementedError("async_stream() must be implemented by subclass")

    async def stop_stream(self):
        """
        Optional override to stop streaming cleanly
        Called only if streaming is active
        """
        pass

# # Important: How subclasses must implement
# # - async_execute for one-time or periodic measurements
# async def async_execute(self, parameters):
#     value = read_sensor()
#     return {"value": value}
# # - async_stream for streaming measurements
# async def async_stream(self, parameters):
#     while True:
#         await asyncio.sleep(1)
#         yield {"count": get_photon_count()}
# # - stop_stream to clean up streaming resources
# async def stop_stream(self):
#     self._streaming = False
