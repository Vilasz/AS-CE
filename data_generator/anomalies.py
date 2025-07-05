import random

def create_temperature_anomaly(value: float) -> float:
    """
    Creates an undeniable temperature anomaly by generating a value in an
    extreme, unrealistic range for a Brazilian climate.
    """
    # Anomaly will be either extremely hot or extremely cold.
    if random.random() > 0.5:
        # Hot anomaly: Unrealistic high temperatures
        return round(random.uniform(45.0, 60.0), 2)
    else:
        # Cold anomaly: Unrealistic freezing temperatures
        return round(random.uniform(-50.0, -10.0), 2)

def create_humidity_anomaly(value: float) -> float:
    """
    Creates a humidity anomaly that is outside the possible range (0-100%).
    This represents a clear sensor malfunction.
    """
    if random.random() > 0.5:
        # Value > 100%
        return round(random.uniform(105.0, 120.0), 2)
    else:
        # Value < 0%
        return round(random.uniform(-20.0, -5.0), 2)

def create_pressure_anomaly(value: float) -> float:
    """
    Creates a significant pressure anomaly by deviating largely from the
    standard atmospheric pressure range.
    """
    # A large deviation from the standard ~1013 hPa
    return round(value + random.choice([-1, 1]) * random.uniform(30, 50), 2)

# A dictionary to easily call the correct function
ANOMALY_FUNCTIONS = {
    "temperature": create_temperature_anomaly,
    "humidity": create_humidity_anomaly,
    "pressure": create_pressure_anomaly,
}