sen2cor_l1c_l2a_config = {
    
    "Id": "",
    "Name": "Sen2Cor_L1C_L2A",
    "Description": "Product processing from Sentinel-2 L1C to L2A. Processor V2.3.6",
    "InputProductType": "S2MSILC",
    "OutputProductType": "S2MSI2A",
    "WorkflowVersion": "0.1",
    "WorkflowOptions": [
        {
            "Name": "Aerosol_Type",
            "Description": "Default processing via configuration is the rural (continental) aerosol type with mid latitude summer and an ozone concentration of 331 Dobson Units",
            "Type": "str",
            "Default": "RURAL",
            "Value": ["MARITIME", "RURAL", "AUTO"],
        },
    ],
}