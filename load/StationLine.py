"""Reader to parse station file"""

from util.parsetext import *


class StationLine:
    """
    Class to parse lines of station history data
    """

    def __init__(self, line: str):
        """
        Create StationLine from text line
        :param line: Line from station history text file
        """
        self.source_id = get_str(line[0:20])
        self.source = get_str(line[21:31])
        self.begin_date = get_str(line[32:40])
        self.end_date = get_str(line[41:49])
        self.station_status = get_str(line[50:70])
        self.ncdcstn_id = get_str(line[71:91])
        self.coop_id = get_str(line[197:217])
        self.ghcnd_id = get_str(line[239:259])
        self.name_pr_sh = get_str(line[361:391])
        self.name_coop_sh = get_str(line[493:523])
        self.nws_climate_div = get_str(line[726:736])
        self.state = get_str(line[778:788])
        self.county = get_str(line[789:839])
        self.nws_st_code = get_str(line[840:842])
        self.fips_country_code = get_str(line[843:845])
        self.nws_region = get_str(line[947:977])
        self.elev_ground = get_float(line[989:1029])
        self.elev_barom = get_float(line[1051:1091])
        self.lat = get_float(line[1299:1319])
        self.lon = get_float(line[1320:1340])
        self.relocation = get_str(line[1352:1414])
        self.utc_offset = get_float(line[1415:1431])
        self.ghcnmlt_id = get_str(line[1574:1594])
        self.county_fips_code = get_str(line[1595:1600])
        self.igra_id = get_str(line[1754:1784])
        self.hpd_id = get_str(line[1785:1805])
