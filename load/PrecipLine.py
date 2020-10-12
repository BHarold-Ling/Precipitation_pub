"""
Class to parse precipitation file line
"""


class PrecipLine:
    """
    Line parser for precipitation data
    """

    def __init__(self, line):
        """
        Init precip_line.
        :param line: Raw input line
        """
        self.station = line[3:9]
        self.state_code = line[3:5]
        self.units = line[15:17]
        self.date1 = line[17:27]

        # Hourly amounts
        worka = []
        count = int(line[27:30])
        for i in range(count - 1):
            start = i * 12 + 30
            hr = line[start:start + 4]
            amt = int(line[start + 4:start + 10])
            flag1 = line[start + 10:start + 11]
            flag2 = line[start + 11:start + 12]

            worka.append((hr, amt, flag1, flag2))
        self.details = worka

        # Daily total
        start = (count - 1) * 12 + 30
        self.daily_tot = int(line[start + 4:start + 10])
        self.daily_flag1 = line[start + 10:start + 11]
        self.daily_flag2 = line[start + 11:start + 12]
