from pprint import pprint

for dev in [
    qepro,
    ultra1,
    ultra2,
    dds1,
    dds1_p1,
    dds1_p2,
    dds2,
    dds2_p1,
    dds2_p2,
    LED,
    deuterium,
    halogen,
    UV_shutter,
]:
    pprint(dev.read())
