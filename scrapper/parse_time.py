import pandas as pd

def parse_time(value):
	if not isinstance(value,str):
		return value
	vls = value.split(' ')

	if vls[0][0:2]=='12':
		if vls[1]=='PM':
			return vls[0]
		else:
			return '00'+vls[0][2:]

	if vls[1]=='AM':
		return vls[0] if vls[0][2]==":"	else "0"+vls[0]
	tms = vls[0].split(':')
	repl = {1:13,2:14,3:15,4:16,5:17,6:18,7:19,8:20,9:21,10:22,11:23}
	tms[0] = str(repl[int(tms[0])])
	return "%s:%s" %(tms[0],tms[1])


df = pd.read_csv("summary.csv")

for i in range(0,len(df)):
	# df.at[i,'timestamp'] = df.at[i,'date']+' '+parse_time(df['time'].get(i))
	df.at[i,'sunset'] = parse_time(df['sunset'].get(i))
	df.at[i,'sunrise'] = parse_time(df['sunrise'].get(i))

df.to_csv("sum.csv")
