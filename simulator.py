"""
DER Device Simulator Module for MCPHub (CLimateweek Hackathon Project).

This module provides simulation capabilities for distributed energy resource (DER) devices
that implement the SunSpec Model Context Protocol (MCP). It allows generating virtual
devices for testing and demonstration purposes.
"""

import logging
import threading
import time
import uuid
import random
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

# Configure logger
logger = logging.getLogger(__name__)

class DeviceSimulator:
    """
    Simulates DER devices that respond to MCP protocol commands.
    
    This class creates virtual devices with realistic data models that can be discovered
    and interacted with through the MCPHub application, useful for testing and demonstrations
    when actual hardware is not available.
    """
    
    def __init__(self, num_devices: int = 3, run_interval: int = 5):
        """
        Initialize the device simulator.
        
        Args:
            num_devices: Number of simulated devices to create
            run_interval: Interval in seconds for updating device data
        """
        self.devices = []
        self.num_devices = num_devices
        self.run_interval = run_interval
        self.simulator_active = False
        self._stop_event = threading.Event()
        
        logger.info(f"Device simulator initialized with {num_devices} devices")
    
    def create_simulated_devices(self):
        """Create a set of simulated devices with different configurations."""
        # Clear existing devices
        self.devices = []
        
        # Always create the Huawei SUN2000 device first
        huawei_device = self._create_huawei_sun2000()
        self.devices.append(huawei_device)
        logger.info(f"Created Huawei {huawei_device['model']} device: {huawei_device['name']}")
        
        # Create additional simulated devices
        for i in range(self.num_devices):
            device_id = str(uuid.uuid4())
            device_name = f"Simulated-DER-{i+1}"
            
            # Determine device type and create appropriate model
            device_type = random.choice(['inverter', 'battery', 'hybrid'])
            
            if device_type == 'inverter':
                device = self._create_inverter(device_id, device_name, i+1)
            elif device_type == 'battery':
                device = self._create_battery(device_id, device_name, i+1)
            else:  # hybrid
                device = self._create_hybrid(device_id, device_name, i+1)
            
            self.devices.append(device)
            logger.info(f"Created simulated {device_type} device: {device_name}")
        
        return self.devices
    
    def _create_inverter(self, device_id: str, device_name: str, index: int) -> Dict[str, Any]:
        """Create a simulated inverter device."""
        num_mppts = random.randint(1, 3)
        
        device = {
            'uuid': device_id,
            'name': device_name,
            'model': f"SIM-INV-{num_mppts}K",
            'manufacturer': "MCPHub Simulator",
            'firmware_version': "1.0.0",
            'protocol_version': "1.0",
            'type': 'inverter',
            'timestamp': datetime.now(),
            'contexts': []
        }
        
        # Add device context
        device_context = {
            'id': 'device',
            'type': 'component',
            'description': "Device-level parameters",
            'modelId': 1,
            'modelName': "Common Model",
            'points': {
                'Pac': {
                    'name': 'AC Power',
                    'type': 'float',
                    'unit': 'W',
                    'access': 'R',
                    'value': 0.0,
                    'description': "Total AC power output"
                },
                'Status': {
                    'name': 'Operating Status',
                    'type': 'uint16',
                    'access': 'R',
                    'value': 1,  # 1 = operating
                    'description': "Current operating status"
                },
                'Temp': {
                    'name': 'Temperature',
                    'type': 'float',
                    'unit': '°C',
                    'access': 'R',
                    'value': 35.0,
                    'description': "Device temperature"
                }
            }
        }
        device['contexts'].append(device_context)
        
        # Add MPPT contexts
        for mppt_idx in range(1, num_mppts + 1):
            mppt_context = {
                'id': f'mppt.{mppt_idx}',
                'type': 'mppt',
                'description': f"Maximum Power Point Tracker {mppt_idx}",
                'modelId': 160,
                'modelName': "MPPT Model",
                'points': {
                    'Pdc': {
                        'name': 'DC Power',
                        'type': 'float',
                        'unit': 'W',
                        'access': 'R',
                        'value': random.uniform(100, 1000),
                        'description': "DC power input"
                    },
                    'Vdc': {
                        'name': 'DC Voltage',
                        'type': 'float',
                        'unit': 'V',
                        'access': 'R',
                        'value': random.uniform(300, 600),
                        'description': "DC voltage input"
                    },
                    'Idc': {
                        'name': 'DC Current',
                        'type': 'float',
                        'unit': 'A',
                        'access': 'R',
                        'value': random.uniform(0.5, 5.0),
                        'description': "DC current input"
                    }
                }
            }
            device['contexts'].append(mppt_context)
        
        # Add AC phase contexts
        num_phases = random.choice([1, 3])
        for phase_idx in range(1, num_phases + 1):
            phase_name = "Single" if num_phases == 1 else f"Phase {phase_idx}"
            phase_context = {
                'id': f'ac.{phase_idx}',
                'type': 'phase',
                'description': f"AC {phase_name}",
                'modelId': 201,
                'modelName': "AC Phase Model",
                'points': {
                    'Pac': {
                        'name': 'AC Power',
                        'type': 'float',
                        'unit': 'W',
                        'access': 'R',
                        'value': random.uniform(50, 800),
                        'description': f"AC power output for {phase_name}"
                    },
                    'Vac': {
                        'name': 'AC Voltage',
                        'type': 'float',
                        'unit': 'V',
                        'access': 'R',
                        'value': random.uniform(220, 240),
                        'description': f"AC voltage for {phase_name}"
                    },
                    'Iac': {
                        'name': 'AC Current',
                        'type': 'float',
                        'unit': 'A',
                        'access': 'R',
                        'value': random.uniform(0.3, 3.0),
                        'description': f"AC current for {phase_name}"
                    },
                    'Freq': {
                        'name': 'Frequency',
                        'type': 'float',
                        'unit': 'Hz',
                        'access': 'R',
                        'value': 50.0 + random.uniform(-0.1, 0.1),
                        'description': "Grid frequency"
                    }
                }
            }
            device['contexts'].append(phase_context)
        
        # Add control context with writable points
        control_context = {
            'id': 'control',
            'type': 'component',
            'description': "Inverter Control",
            'modelId': 123,
            'modelName': "Control Model",
            'points': {
                'WMaxLim': {
                    'name': 'Active Power Limit',
                    'type': 'float',
                    'unit': '%',
                    'access': 'RW',
                    'value': 100.0,
                    'description': "Maximum active power output limit (percentage)"
                },
                'Ena': {
                    'name': 'Enable/Disable Inverter',
                    'type': 'uint16',
                    'access': 'RW',
                    'value': 1,  # 1 = enabled
                    'description': "Enable (1) or disable (0) the inverter"
                }
            }
        }
        device['contexts'].append(control_context)
        
        return device
    
    def _create_battery(self, device_id: str, device_name: str, index: int) -> Dict[str, Any]:
        """Create a simulated battery storage device."""
        capacity = random.choice([5, 10, 15])
        
        device = {
            'uuid': device_id,
            'name': device_name,
            'model': f"SIM-BAT-{capacity}kWh",
            'manufacturer': "MCPHub Simulator",
            'firmware_version': "1.0.0",
            'protocol_version': "1.0",
            'type': 'battery',
            'timestamp': datetime.now(),
            'contexts': []
        }
        
        # Add device context
        device_context = {
            'id': 'device',
            'type': 'component',
            'description': "Device-level parameters",
            'modelId': 1,
            'modelName': "Common Model",
            'points': {
                'Status': {
                    'name': 'Operating Status',
                    'type': 'uint16',
                    'access': 'R',
                    'value': 1,  # 1 = operating
                    'description': "Current operating status"
                },
                'Temp': {
                    'name': 'Temperature',
                    'type': 'float',
                    'unit': '°C',
                    'access': 'R',
                    'value': 28.0,
                    'description': "Device temperature"
                }
            }
        }
        device['contexts'].append(device_context)
        
        # Add battery context
        battery_context = {
            'id': 'storage',
            'type': 'storage',
            'description': "Battery Storage",
            'modelId': 802,
            'modelName': "Storage Model",
            'points': {
                'SoC': {
                    'name': 'State of Charge',
                    'type': 'float',
                    'unit': '%',
                    'access': 'R',
                    'value': random.uniform(20, 90),
                    'description': "Battery state of charge"
                },
                'W': {
                    'name': 'Power',
                    'type': 'float',
                    'unit': 'W',
                    'access': 'R',
                    'value': random.uniform(-500, 500),  # Negative = charging
                    'description': "Battery power (negative = charging, positive = discharging)"
                },
                'V': {
                    'name': 'Voltage',
                    'type': 'float',
                    'unit': 'V',
                    'access': 'R',
                    'value': random.uniform(45, 55),
                    'description': "Battery voltage"
                },
                'ChaState': {
                    'name': 'Charging State',
                    'type': 'uint16',
                    'access': 'R',
                    'value': random.choice([1, 2, 3]),  # 1=idle, 2=charging, 3=discharging
                    'description': "Battery charging state"
                },
                'Health': {
                    'name': 'Battery Health',
                    'type': 'float',
                    'unit': '%',
                    'access': 'R',
                    'value': random.uniform(90, 100),
                    'description': "Battery health percentage"
                }
            }
        }
        device['contexts'].append(battery_context)
        
        # Add control context with writable points
        control_context = {
            'id': 'control',
            'type': 'component',
            'description': "Battery Control",
            'modelId': 123,
            'modelName': "Control Model",
            'points': {
                'WChaGra': {
                    'name': 'Charge Rate',
                    'type': 'float',
                    'unit': '%',
                    'access': 'RW',
                    'value': 50.0,
                    'description': "Battery charging rate limit"
                },
                'WDisChaGra': {
                    'name': 'Discharge Rate',
                    'type': 'float',
                    'unit': '%',
                    'access': 'RW',
                    'value': 50.0,
                    'description': "Battery discharging rate limit"
                },
                'StorCtl_Mod': {
                    'name': 'Storage Control Mode',
                    'type': 'uint16',
                    'access': 'RW',
                    'value': 1,  # 1=auto, 2=charge, 3=discharge
                    'description': "Battery operation mode"
                }
            }
        }
        device['contexts'].append(control_context)
        
        return device
    
    def _create_huawei_sun2000(self) -> Dict[str, Any]:
        """Create a simulated Huawei SUN2000-40KTL-M3 device with real specifications."""
        # Use the provided information for the Huawei device
        device_id = "1"  # As provided by user
        device_name = "Huawei SUN2000"
        
        device = {
            'uuid': device_id,
            'name': device_name,
            'model': "SUN2000-40KTL-M3",  # As provided by user
            'manufacturer': "Huawei",  # As provided by user
            'firmware_version': "V100R001D02",  # As provided by user
            'serial_number': "ES2340051644",  # As provided by user
            'protocol_version': "1.0",
            'type': 'inverter',
            'timestamp': datetime.now(),
            'contexts': []
        }
        
        # Add device context
        device_context = {
            'id': 'device',
            'type': 'component',
            'description': "Device-level parameters",
            'modelId': 1,
            'modelName': "Common Model",
            'points': {
                'Pac': {
                    'name': 'AC Power',
                    'type': 'float',
                    'unit': 'W',
                    'access': 'R',
                    'value': random.uniform(15000, 38000),  # Up to 40kW rating
                    'description': "Total AC power output"
                },
                'Status': {
                    'name': 'Operating Status',
                    'type': 'uint16',
                    'access': 'R',
                    'value': 1,  # 1 = operating
                    'description': "Current operating status"
                },
                'Temp': {
                    'name': 'Temperature',
                    'type': 'float',
                    'unit': '°C',
                    'access': 'R',
                    'value': random.uniform(40, 55),  # Inverters run hot
                    'description': "Device temperature"
                },
                'SN': {
                    'name': 'Serial Number',
                    'type': 'string',
                    'access': 'R',
                    'value': "ES2340051644",
                    'description': "Device serial number"
                }
            }
        }
        device['contexts'].append(device_context)
        
        # Add MPPT contexts - SUN2000-40KTL-M3 has 8 MPPTs
        for mppt_idx in range(1, 9):
            mppt_context = {
                'id': f'mppt.{mppt_idx}',
                'type': 'mppt',
                'description': f"Maximum Power Point Tracker {mppt_idx}",
                'modelId': 160,
                'modelName': "MPPT Model",
                'points': {
                    'Pdc': {
                        'name': 'DC Power',
                        'type': 'float',
                        'unit': 'W',
                        'access': 'R',
                        'value': random.uniform(2000, 5500),  # Each MPPT up to 6kW
                        'description': "DC power input"
                    },
                    'Vdc': {
                        'name': 'DC Voltage',
                        'type': 'float',
                        'unit': 'V',
                        'access': 'R',
                        'value': random.uniform(550, 650),  # Higher voltage for commercial
                        'description': "DC voltage input"
                    },
                    'Idc': {
                        'name': 'DC Current',
                        'type': 'float',
                        'unit': 'A',
                        'access': 'R',
                        'value': random.uniform(4.0, 8.5),  # Higher current
                        'description': "DC current input"
                    }
                }
            }
            device['contexts'].append(mppt_context)
        
        # Add AC phase contexts - Three-phase inverter
        for phase_idx in range(1, 4):
            phase_name = f"Phase {phase_idx}"
            phase_context = {
                'id': f'ac.{phase_idx}',
                'type': 'phase',
                'description': f"AC {phase_name}",
                'modelId': 201,
                'modelName': "AC Phase Model",
                'points': {
                    'Pac': {
                        'name': 'AC Power',
                        'type': 'float',
                        'unit': 'W',
                        'access': 'R',
                        'value': random.uniform(5000, 13000),  # Balanced across 3 phases
                        'description': f"AC power output for {phase_name}"
                    },
                    'Vac': {
                        'name': 'AC Voltage',
                        'type': 'float',
                        'unit': 'V',
                        'access': 'R',
                        'value': random.uniform(225, 235),  # Tight voltage range
                        'description': f"AC voltage for {phase_name}"
                    },
                    'Iac': {
                        'name': 'AC Current',
                        'type': 'float',
                        'unit': 'A',
                        'access': 'R',
                        'value': random.uniform(18.0, 25.0),  # Higher current for commercial
                        'description': f"AC current for {phase_name}"
                    },
                    'Freq': {
                        'name': 'Frequency',
                        'type': 'float',
                        'unit': 'Hz',
                        'access': 'R',
                        'value': 50.0 + random.uniform(-0.05, 0.05),  # Tighter frequency range
                        'description': "Grid frequency"
                    },
                    'PF': {
                        'name': 'Power Factor',
                        'type': 'float',
                        'access': 'R',
                        'value': random.uniform(0.97, 1.00),  # Good power factor
                        'description': "Power factor"
                    }
                }
            }
            device['contexts'].append(phase_context)
        
        # Add control context with writable points
        control_context = {
            'id': 'control',
            'type': 'component',
            'description': "Inverter Control",
            'modelId': 123,
            'modelName': "Control Model",
            'points': {
                'WMaxLim': {
                    'name': 'Active Power Limit',
                    'type': 'float',
                    'unit': '%',
                    'access': 'RW',
                    'value': 100.0,
                    'description': "Maximum active power output limit (percentage)"
                },
                'VarMaxLim': {
                    'name': 'Reactive Power Limit',
                    'type': 'float',
                    'unit': '%',
                    'access': 'RW',
                    'value': 50.0,
                    'description': "Maximum reactive power output limit (percentage)"
                },
                'Ena': {
                    'name': 'Enable/Disable Inverter',
                    'type': 'uint16',
                    'access': 'RW',
                    'value': 1,  # 1 = enabled
                    'description': "Enable (1) or disable (0) the inverter"
                },
                'VoltVar': {
                    'name': 'Volt-Var Mode',
                    'type': 'uint16',
                    'access': 'RW',
                    'value': 0,  # 0 = disabled
                    'description': "Volt-Var mode (0=off, 1=on)"
                }
            }
        }
        device['contexts'].append(control_context)
        
        return device
        
    def _create_hybrid(self, device_id: str, device_name: str, index: int) -> Dict[str, Any]:
        """Create a simulated hybrid inverter+battery device."""
        num_mppts = random.randint(1, 2)
        battery_capacity = random.choice([5, 10])
        
        device = {
            'uuid': device_id,
            'name': device_name,
            'model': f"SIM-HYB-{num_mppts}K-{battery_capacity}kWh",
            'manufacturer': "MCPHub Simulator",
            'firmware_version': "1.0.0",
            'protocol_version': "1.0",
            'type': 'hybrid',
            'timestamp': datetime.now(),
            'contexts': []
        }
        
        # Add device context
        device_context = {
            'id': 'device',
            'type': 'component',
            'description': "Device-level parameters",
            'modelId': 1,
            'modelName': "Common Model",
            'points': {
                'Pac': {
                    'name': 'AC Power',
                    'type': 'float',
                    'unit': 'W',
                    'access': 'R',
                    'value': 0.0,
                    'description': "Total AC power output"
                },
                'Status': {
                    'name': 'Operating Status',
                    'type': 'uint16',
                    'access': 'R',
                    'value': 1,  # 1 = operating
                    'description': "Current operating status"
                },
                'Temp': {
                    'name': 'Temperature',
                    'type': 'float',
                    'unit': '°C',
                    'access': 'R',
                    'value': 38.0,
                    'description': "Device temperature"
                }
            }
        }
        device['contexts'].append(device_context)
        
        # Add MPPT contexts
        for mppt_idx in range(1, num_mppts + 1):
            mppt_context = {
                'id': f'mppt.{mppt_idx}',
                'type': 'mppt',
                'description': f"Maximum Power Point Tracker {mppt_idx}",
                'modelId': 160,
                'modelName': "MPPT Model",
                'points': {
                    'Pdc': {
                        'name': 'DC Power',
                        'type': 'float',
                        'unit': 'W',
                        'access': 'R',
                        'value': random.uniform(100, 800),
                        'description': "DC power input"
                    },
                    'Vdc': {
                        'name': 'DC Voltage',
                        'type': 'float',
                        'unit': 'V',
                        'access': 'R',
                        'value': random.uniform(300, 600),
                        'description': "DC voltage input"
                    },
                    'Idc': {
                        'name': 'DC Current',
                        'type': 'float',
                        'unit': 'A',
                        'access': 'R',
                        'value': random.uniform(0.5, 5.0),
                        'description': "DC current input"
                    }
                }
            }
            device['contexts'].append(mppt_context)
        
        # Add AC phase context
        phase_context = {
            'id': 'ac.1',
            'type': 'phase',
            'description': f"AC Output",
            'modelId': 201,
            'modelName': "AC Phase Model",
            'points': {
                'Pac': {
                    'name': 'AC Power',
                    'type': 'float',
                    'unit': 'W',
                    'access': 'R',
                    'value': random.uniform(50, 800),
                    'description': f"AC power output"
                },
                'Vac': {
                    'name': 'AC Voltage',
                    'type': 'float',
                    'unit': 'V',
                    'access': 'R',
                    'value': random.uniform(220, 240),
                    'description': f"AC voltage"
                },
                'Iac': {
                    'name': 'AC Current',
                    'type': 'float',
                    'unit': 'A',
                    'access': 'R',
                    'value': random.uniform(0.3, 3.0),
                    'description': f"AC current"
                },
                'Freq': {
                    'name': 'Frequency',
                    'type': 'float',
                    'unit': 'Hz',
                    'access': 'R',
                    'value': 50.0 + random.uniform(-0.1, 0.1),
                    'description': "Grid frequency"
                }
            }
        }
        device['contexts'].append(phase_context)
        
        # Add battery context
        battery_context = {
            'id': 'storage',
            'type': 'storage',
            'description': "Battery Storage",
            'modelId': 802,
            'modelName': "Storage Model",
            'points': {
                'SoC': {
                    'name': 'State of Charge',
                    'type': 'float',
                    'unit': '%',
                    'access': 'R',
                    'value': random.uniform(20, 90),
                    'description': "Battery state of charge"
                },
                'W': {
                    'name': 'Power',
                    'type': 'float',
                    'unit': 'W',
                    'access': 'R',
                    'value': random.uniform(-500, 500),  # Negative = charging
                    'description': "Battery power (negative = charging, positive = discharging)"
                },
                'V': {
                    'name': 'Voltage',
                    'type': 'float',
                    'unit': 'V',
                    'access': 'R',
                    'value': random.uniform(45, 55),
                    'description': "Battery voltage"
                },
                'ChaState': {
                    'name': 'Charging State',
                    'type': 'uint16',
                    'access': 'R',
                    'value': random.choice([1, 2, 3]),  # 1=idle, 2=charging, 3=discharging
                    'description': "Battery charging state"
                }
            }
        }
        device['contexts'].append(battery_context)
        
        # Add control context with writable points
        control_context = {
            'id': 'control',
            'type': 'component',
            'description': "System Control",
            'modelId': 123,
            'modelName': "Control Model",
            'points': {
                'WMaxLim': {
                    'name': 'Active Power Limit',
                    'type': 'float',
                    'unit': '%',
                    'access': 'RW',
                    'value': 100.0,
                    'description': "Maximum active power output limit (percentage)"
                },
                'StorCtl_Mod': {
                    'name': 'Storage Control Mode',
                    'type': 'uint16',
                    'access': 'RW',
                    'value': 1,  # 1=auto, 2=charge, 3=discharge
                    'description': "Battery operation mode"
                },
                'Ena': {
                    'name': 'Enable/Disable System',
                    'type': 'uint16',
                    'access': 'RW',
                    'value': 1,  # 1 = enabled
                    'description': "Enable (1) or disable (0) the system"
                }
            }
        }
        device['contexts'].append(control_context)
        
        return device
    
    def update_device_data(self):
        """Update the simulated device data with realistic changes over time."""
        for device in self.devices:
            # Update timestamp
            device['timestamp'] = datetime.now()
            
            # Update each context and its data points
            for context in device['contexts']:
                # Find data points that could change over time
                if context['type'] == 'mppt':
                    # Update MPPT values with small variations
                    pdc = context['points'].get('Pdc', {})
                    if pdc:
                        current_value = pdc.get('value', 0)
                        # Add a small random variation
                        new_value = current_value * random.uniform(0.9, 1.1)
                        # Keep within realistic bounds
                        new_value = max(50, min(1200, new_value))
                        pdc['value'] = new_value
                    
                    # Update related values
                    vdc = context['points'].get('Vdc', {})
                    idc = context['points'].get('Idc', {})
                    
                    if vdc and idc and pdc:
                        # Adjust voltage with small variation
                        vdc['value'] = vdc.get('value', 400) * random.uniform(0.98, 1.02)
                        # Calculate current based on power and voltage (P = V * I)
                        if vdc['value'] > 0:
                            idc['value'] = pdc['value'] / vdc['value']
                
                elif context['type'] == 'phase':
                    # Update AC values with small variations
                    pac = context['points'].get('Pac', {})
                    if pac:
                        current_value = pac.get('value', 0)
                        # Add a small random variation
                        new_value = current_value * random.uniform(0.95, 1.05)
                        # Keep within realistic bounds
                        new_value = max(30, min(1000, new_value))
                        pac['value'] = new_value
                    
                    # Update related values
                    vac = context['points'].get('Vac', {})
                    iac = context['points'].get('Iac', {})
                    freq = context['points'].get('Freq', {})
                    
                    if vac:
                        # Small voltage variations
                        vac['value'] = 230 + random.uniform(-5, 5)
                    
                    if iac and vac and pac:
                        # Calculate current based on power and voltage
                        if vac['value'] > 0:
                            iac['value'] = pac['value'] / vac['value']
                    
                    if freq:
                        # Small frequency variations
                        freq['value'] = 50 + random.uniform(-0.1, 0.1)
                
                elif context['type'] == 'storage':
                    # Update battery values
                    soc = context['points'].get('SoC', {})
                    power = context['points'].get('W', {})
                    state = context['points'].get('ChaState', {})
                    
                    if soc and power and state:
                        # Determine if charging or discharging
                        current_soc = soc.get('value', 50)
                        current_state = state.get('value', 1)
                        
                        # Change state occasionally
                        if random.random() < 0.1:
                            if current_soc < 20:
                                # Low battery, start charging
                                new_state = 2  # charging
                            elif current_soc > 90:
                                # High battery, start discharging
                                new_state = 3  # discharging
                            else:
                                # Random state
                                new_state = random.choice([1, 2, 3])
                            state['value'] = new_state
                        
                        # Update power based on state
                        if state['value'] == 2:  # Charging
                            power['value'] = -random.uniform(100, 800)
                            # Increase SoC
                            soc['value'] = min(100, current_soc + random.uniform(0.1, 0.5))
                        elif state['value'] == 3:  # Discharging
                            power['value'] = random.uniform(100, 800)
                            # Decrease SoC
                            soc['value'] = max(0, current_soc - random.uniform(0.1, 0.5))
                        else:  # Idle
                            power['value'] = random.uniform(-20, 20)
                            # Minimal change
                            soc['value'] = current_soc + random.uniform(-0.1, 0.1)
                
                elif context['id'] == 'device':
                    # Update device-level values
                    temp = context['points'].get('Temp', {})
                    
                    if temp:
                        # Small temperature variations
                        current_temp = temp.get('value', 35)
                        temp['value'] = current_temp + random.uniform(-0.5, 0.5)
            
            # Update device-level power if exists (sum of all contexts)
            device_context = next((c for c in device['contexts'] if c['id'] == 'device'), None)
            if device_context and 'Pac' in device_context['points']:
                total_power = 0
                # Sum power from all phase contexts
                for context in device['contexts']:
                    if context['type'] == 'phase' and 'Pac' in context['points']:
                        total_power += context['points']['Pac'].get('value', 0)
                
                device_context['points']['Pac']['value'] = total_power
    
    def start_simulator(self):
        """Start the simulator in the current thread."""
        logger.info("Starting device simulator")
        self.simulator_active = True
        self._stop_event.clear()
        
        # Create simulated devices
        self.create_simulated_devices()
        
        try:
            while not self._stop_event.is_set():
                try:
                    # Update device data
                    self.update_device_data()
                    logger.debug(f"Updated data for {len(self.devices)} simulated devices")
                except Exception as e:
                    logger.error(f"Error in simulator cycle: {str(e)}")
                
                # Wait for the next update cycle or until stopped
                self._stop_event.wait(self.run_interval)
        
        finally:
            self.simulator_active = False
            logger.info("Device simulator stopped")
    
    def stop_simulator(self):
        """Stop the simulator."""
        if self.simulator_active:
            logger.info("Stopping device simulator")
            self._stop_event.set()
    
    def get_devices(self) -> List[Dict[str, Any]]:
        """Get the list of simulated devices."""
        return self.devices
    
    def get_device_by_uuid(self, uuid: str) -> Optional[Dict[str, Any]]:
        """Get a simulated device by its UUID."""
        for device in self.devices:
            if device['uuid'] == uuid:
                return device
        return None
    
    def handle_discovery_request(self, source_ip: str) -> List[Dict[str, Any]]:
        """
        Handle a discovery request from the specified source IP.
        
        Args:
            source_ip: The source IP address of the request
            
        Returns:
            List of devices to be discovered from this request
        """
        # In a real simulator, we might filter based on the source IP
        # Here we'll just return all devices
        return self.devices
    
    def handle_read_request(self, device_uuid: str, context_id: str, point_ids: List[str] = None) -> Dict[str, Any]:
        """
        Handle a read request for a specific device, context, and points.
        
        Args:
            device_uuid: The device UUID
            context_id: The context ID
            point_ids: Optional list of point IDs to read
            
        Returns:
            Dict containing the result of the operation
        """
        device = self.get_device_by_uuid(device_uuid)
        
        if not device:
            return {
                'success': False,
                'error': f"Device with UUID {device_uuid} not found"
            }
        
        # Find the requested context
        context = next((c for c in device['contexts'] if c['id'] == context_id), None)
        
        if not context:
            return {
                'success': False,
                'error': f"Context with ID {context_id} not found in device {device_uuid}"
            }
        
        # Extract requested points
        points_data = {}
        if point_ids:
            # Read only specified points
            for point_id in point_ids:
                if point_id in context['points']:
                    points_data[point_id] = context['points'][point_id]['value']
        else:
            # Read all points
            for point_id, point_info in context['points'].items():
                points_data[point_id] = point_info['value']
        
        return {
            'success': True,
            'device_uuid': device_uuid,
            'context_id': context_id,
            'data': points_data,
            'timestamp': datetime.now().isoformat()
        }
    
    def handle_write_request(self, device_uuid: str, context_id: str, 
                           points_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a write request for a specific device, context, and points.
        
        Args:
            device_uuid: The device UUID
            context_id: The context ID
            points_data: Dictionary of point_id -> value pairs to write
            
        Returns:
            Dict containing the result of the operation
        """
        device = self.get_device_by_uuid(device_uuid)
        
        if not device:
            return {
                'success': False,
                'error': f"Device with UUID {device_uuid} not found"
            }
        
        # Find the requested context
        context = next((c for c in device['contexts'] if c['id'] == context_id), None)
        
        if not context:
            return {
                'success': False,
                'error': f"Context with ID {context_id} not found in device {device_uuid}"
            }
        
        # Update the points
        updated_points = {}
        for point_id, value in points_data.items():
            if point_id in context['points']:
                point = context['points'][point_id]
                
                # Check if point is writable
                if point.get('access') in ['W', 'RW']:
                    # Update point value
                    point['value'] = value
                    updated_points[point_id] = value
                else:
                    return {
                        'success': False,
                        'error': f"Point {point_id} is not writable (access: {point.get('access')})"
                    }
            else:
                return {
                    'success': False,
                    'error': f"Point {point_id} not found in context {context_id}"
                }
        
        return {
            'success': True,
            'device_uuid': device_uuid,
            'context_id': context_id,
            'updated_points': updated_points,
            'timestamp': datetime.now().isoformat()
        }

# Singleton instance for use across the application
simulator = None

def init_simulator(num_devices: int = 3, run_interval: int = 5):
    """Initialize the device simulator with the specified parameters."""
    global simulator
    
    if simulator is None:
        simulator = DeviceSimulator(num_devices, run_interval)
    
    return simulator

def get_simulator() -> Optional[DeviceSimulator]:
    """Get the singleton simulator instance."""
    return simulator
