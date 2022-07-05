###########################################################################################
# This file is a stub for eventually extracting publication stuff from other classes.
# This would potentially allow far fewer subclasses to be necessary for other dependencies.
# The publisher class could be moved into the library and do something not specific to
# VOLTTRON, while the TNSPublisher would remain in the TransactiveNodeAgent.
# TODO: Can the data_manager functionality be rolled into this as well, perhaps as part of
#  the base class?
###########################################################################################


class Publisher(object):
    def __init__(self):
        pass

class TNSPublisher(Publisher):
    def __init__(self, *args, **kwargs):
        super(TNSPublisher, self).__init__(*args, **kargs)
        pass

    def publish(self):
        pass