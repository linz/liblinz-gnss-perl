import sys
import argparse
import datetime
import csv


def main():
    parser=argparse.ArgumentParser(description="Parse requests in getdata log")
    parser.add_argument("getdata_log_file",help="Debug log from getdata")
    parser.add_argument("csv_summary",help="Output CSV summary file")
    args=parser.parse_args()
    with open(args.getdata_log_file) as logh, open(args.csv_summary,'w') as csvh:
        csvw=csv.writer(csvh)
        csvw.writerow('datetime datacenter request filename status seconds'.split())
        datacenter=None
        filename=''
        for record in logh:
            parts=record.split(maxsplit=2)
            if len(parts) < 3:
                continue
            rdate,rtime,message=parts
            try:
                logtime=datetime.datetime.strptime(f"{rdate} {rtime}","%Y/%m/%d %H:%M:%S")
            except:
                continue
            if message.startswith("Running getData"):
                starttime=logtime
                parts=message.split()
                datacenter=parts[3]
                request=parts[8]
            elif message.startswith("Retrieving file "):
                filename=message[16:].strip()
            elif message.startswith("Returning status"):
                parts=message.split()
                status=parts[2][:4]
                if datacenter is not None:
                    seconds=str((logtime-starttime).total_seconds())
                    csvw.writerow((starttime.strftime("%Y-%m-%d %H:%M:%S"),datacenter,request,filename,status,seconds))
                    datacenter=None
                    filename=''

if __name__=="__main__":
    main()
