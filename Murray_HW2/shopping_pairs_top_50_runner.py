
# Top 50 items pairs by frequency:

from shopping_pairs_top_50 import ShoppingPairsTop50
mr_job = ShoppingPairsTop50(args=['ProductPurchaseData.txt', '--jobconf="mapred.map.tasks=4"', '--jobconf="mapred.reduce.tasks=4"']) 
with mr_job.make_runner() as runner: 
    runner.run()
    count = 0
    for line in runner.stream_output():
        key, value =  mr_job.parse_output_line(line)
        print '{}, {}, {}, {}'.format(key[0], key[1], value[0], value[1])
        count += 1
        if count > 49:
            break
    print runner.counters()