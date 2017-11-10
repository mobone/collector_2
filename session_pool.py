from requests_toolbelt.threaded import pool

urls = []

for i in range(1,20*82,20):
    urls.append('https://finviz.com/screener.ashx?v=111&f=cap_smallover,sh_avgvol_o500,sh_opt_option&r=%s' % i)

p = pool.Pool.from_urls(urls, num_processes=20)
p.join_all()


for response in p.responses():
    print('GET {0}. Returned {1}.'.format(response.request_kwargs['url'],
                                          response.status_code))
print(dir(p))
