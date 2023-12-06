'''
Created on Jun 24, 2020

@author: bsana
'''

AGG_DICT = {
    # California St E
    169566458: [169566458, 169472188, 441597628, 441597637, 441597642, 441597658, 441597660],
    449830986: list(range(449830986, 449830991)) + [441599059], 
    441599764: [441599764] + list(range(449831810, 449831817)),
    
    # California St W
    441597661: [441597661, 441597659, 441597643, 441597640, 441597629, 169791261, 169715550],
    441599060: [441599060] + list(range(449830991, 449830996)),
    449831817: list(range(449831817, 449831824)) + [441599765],
    
    # Geary Blvd E
    441565716: [441565716, 441565719, 441565728, 441565730, 441565776, 441565779, 441565810, 1626733846, 441639113],
    449825265: [449825265, 449825266],
    449825267: [449825267, 441565836] + list(range(449830946, 449830951)),
    449830973: list(range(449830973, 449830982)) + [441598530, 1626634605],
    1626634726: [1626634726, 441598887] + list(range(449831714, 449831725)),
    
    # Geary Blvd W
    441565811: [441565811, 441565780, 441565777, 441565735, 441565729, 441565720, 441565717],
    449825269: [449825269, 449825270],
    449850143: list(range(449850143, 449850148)) + [441637970, 449825268],
    1626712869: [1626712869, 441637971] + list(range(449850148, 449850157)),
    449850157: list(range(449850157, 449850168)) + [441637972, 1626767000],
    
    # 3rd Street N
    449850031: list(range(449850031, 449850036)),
    449826953: list(range(449826953, 449826959)) + [449826961, 449830430, 449830431],
    449853382: list(range(449853382, 449853385)) + [449826950, 449826951],
    
    # 3rd Street S
    449830437: list(range(449830437, 449830442)),
    449850028: list(range(449850028, 449850031)) + list(range(449850024, 449850028)) + [449850022, 449850023],
    449850020: [449850020, 449850021] + list(range(449850017, 449850020)), 
    
    # Fulton St E
    449825294: [449825294, 449825295],
    449825296: [449825296, 169359939, 449825321, 449825322],
    449830943: list(range(449830943, 449830946)),
    
    # Fulton St W
    449830940: list(range(449830940, 449830943)),
    449825323: [449825323, 449825324, 170013254, 449825291],
    449825292: [449825292, 449825293],
    
    # Sunset Blvd S
    171340876 : [171340876, 171340877],
    449851159: [449851159, 449851160],
    449851161: list(range(449851161, 449851166)) + [170209906],
    441639188: [441639188, 171340878],
    
    # Sunset Blvd N
    169810924: [169810924, 1626682364],
    441564125: [441564125, 169172082],
    449825178: [449825178, 449825179] + list(range(449825239, 449825242)),
    449825250: [449825250, 449825251],
    1626754164: [1626754164, 1626683648]
    
}