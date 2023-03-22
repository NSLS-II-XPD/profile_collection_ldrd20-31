class syrng_ultra(Device):
    status = Cpt(EpicsSignalRO, 'STATUS', string=True)
    # 0: Idle
    # 1: Infusing
    # 2: Withdrawing
    # 4: Target Reached
    
    communication = Cpt(EpicsSignal, 'DISABLE', string=True)
    # 1: DISABLE
    # 0: Enable
    
    update_pump = Cpt(EpicsSignal, 'UPDATE.SCAN', string=True)
    # 0: Passive
    # 1: Event
    # 2: I/O Intr
    # 3: 10 second
    # 4: 5 second
    # 5: 2 second
    # 6: 1 second
    # 7: .5 second
    # 8: .2 second
    # 9: .1 second
    
    pump_infuse = Cpt(EpicsSignal, 'IRUN', string=True)
    pump_withdraw = Cpt(EpicsSignal, 'WRUN', string=True)
    pump_stop = Cpt(EpicsSignal, 'STOP', string=True)
    
    
    target_vol = Cpt(EpicsSignal, 'TVOLUME')
    target_vol_unit = Cpt(EpicsSignal, 'TVOLUMEUNITS', string=True)
    read_target_vol = Cpt(EpicsSignalRO, 'TVOLUME:RBV', string=True)
    read_target_vol_unit = Cpt(EpicsSignalRO, 'TVOLUMEUNITS:RBV', string=True)
    
    clear_infused = Cpt(EpicsSignal, 'CLEARINFUSED', string=True)
    clear_withdrawn = Cpt(EpicsSignal, 'CLEARWITHDRAWN', string=True)
    
    read_infused = Cpt(EpicsSignalRO, 'IVOLUME:RBV', string=True)
    read_withdrawn = Cpt(EpicsSignalRO, 'WVOLUME:RBV', string=True)
    read_infused_unit = Cpt(EpicsSignalRO, 'TVOLUMEUNITS:RBV', string=True)
    read_withdrawn_unit = Cpt(EpicsSignalRO, 'WVOLUMEUNITS:RBV', string=True)



ultra1 = syrng_ultra('XF:28IDC-ES:1{Pump:Syrng-Ultra:1}:', name='Pump_Ultra1', 
                     read_attrs=['status', 'communication', 'update_pump', 'read_target_vol', 'read_target_vol_unit',
                                 'read_infused', 'read_infused_unit', 'read_withdrawn', 'read_withdrawn_unit'])
