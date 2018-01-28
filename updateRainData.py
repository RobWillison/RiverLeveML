from DataGatherer import RainGatherer
from DataGatherer import RainForecastGatherer
from DataGatherer import DarkSkyGatherer


# DarkSkyGatherer.collectPastDay([1,2780,2779,2676,2666,2665,2408,2382,2117,2781,2671,2417,2750,2708,2790,2116,2749,2411])
# DarkSkyGatherer.collectForecastData([1,2780,2779,2676,2666,2665,2408,2382,2117,2781,2671,2417,2750,2708,2790,2116,2749,2411])
RainGatherer.doWork()
RainForecastGatherer.doWork()
# RiverLevelGatherer.checkForUpdates()
