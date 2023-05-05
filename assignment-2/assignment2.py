import csv
def compute_waterbalance(soiltype):
    if(soiltype=="Deep"):
        C=100
        gamma=0.2
    if(soiltype=="Shallow"):
        C=42
        gamma=0.4
    with open("daily_rainfall_jalgaon_chalisgaon_talegaon_2022.csv",'r') as f:
        reader_obj=csv.reader(f)
        all_rows=[]
        for r in reader_obj:
            all_rows.append(r)
    # print(all_rows)
    all_rows=all_rows[1:]
    result=[]
    #actual code
    for i in range(len(all_rows)):
        rainfall=float(all_rows[i][1])
        if(0<=rainfall and rainfall<25):
            runoff=0.2*rainfall
        elif(25<=rainfall and rainfall<50):
            runoff=0.3*rainfall
        elif(50<=rainfall and rainfall<75):
            runoff=0.4*rainfall
        elif(75<=rainfall and rainfall<100):
            runoff=0.5*rainfall
        elif(rainfall>=100):
            runoff=0.7*rainfall             #runoff computed here
        #infiltration= today infiltrated water + yesterday's sm
        today_infiltration=rainfall-runoff
        if(i==0):
            infiltration=0+today_infiltration
        else:
            infiltration=result[i-1][4]+today_infiltration
        #if enough water is infiltrated then crop takes 4mm else takes up the infiltraion thus remaining attributes will also be 0.
        if(infiltration>=4):
            uptake=4
        else:
            uptake=infiltration
            infiltraion=0
            result.append([all_rows[i][0],rainfall,runoff+0,uptake,0,0])
            continue
        infiltration=infiltration-uptake
        #gw is calculated based on gamma
        gw=infiltration*gamma
        infiltration=infiltration-gw
        if(infiltration>C):
            excess=infiltration-C
        else:
            excess=0
        infiltration=infiltration-excess
        #at the end remaining infilration is taken as gw
        sm=infiltration
        result.append([all_rows[i][0],rainfall,runoff+excess,uptake,sm,gw])
    r=d=f=s=0
    outheader=["date","rain_mm","runoff+excess","crop uptake","Soil Moisture(SM)","Percolation to Ground Water(GW)"]
    with open("output/"+str(soiltype)+"_output.csv",'w') as file_ptr:
        writer=csv.writer(file_ptr)
        writer.writerow(outheader)
        for row in result:
            writer.writerow(row)
            r+=row[1]
            d+=row[2]
            f+=row[3]
            s+=row[5]
    #vaidating the results by sum
    print("total rain fall is :"+str(r))
    print("sum of all runoff,excess,uptake and groundwater and soil_moisture(n) is  :"+str(d+f+s+result[len(result)-1][4]))
compute_waterbalance("Deep")