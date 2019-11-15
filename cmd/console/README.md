# Baudb Command line Operation Tool

./console -h 127.0.0.1 -p 8089                                                
127.0.0.1:8089> slaveof 127.0.0.1 8088             

    ok                                                                         
127.0.0.1:8089> info                            
    
    Shard: 9080f1567c4411e984fd54e1adcd41e8
    IP: 10.12.161.78 
    Port: 8088
    DiskFree: 824GB
    IDC: 
    SucceedSel: 3
    FailedSel: 0
    SucceedLVals: 0
    FailedLVals: 0
    ReceivedAdd: 1900
    SucceedAdd: 1900
    FailedAdd: 0
    OutOfOrder: 0
    AmendSample: 0
    OutOfBounds: 0
    Committed: 1900
    FailedCommit: 0
    SeriesNum: 1
    BlockNum: 0
    HeadMinTime: 2019-12-10T18:10:07+08:00
    HeadMaxTime: 2019-12-10T18:10:09+08:00
    HeadMinValidTime: -9223372036854775808
    AppenderMinValidTime: 2019-12-10T18:09:54+08:00
    LastRecvHb: 0
    LastSendHb: 0          
                                                        
127.0.0.1:8089> write {app="mysql",idc="langfang"} "hello, word!"    
    
    ok        
127.0.0.1:8089> write {app="mysql",idc="langfang"} "baudb started."   

    ok                    
127.0.0.1:8088> select {app="mysql"} 100

    {
    	"resultType": "logStreams",
    	"result": [
    		{
    			"Labels": [
    				{
    					"Name": "app",
    					"Value": "mysql"
    				},
    				{
    					"Name": "idc",
    					"Value": "langfang"
    				}
    			],
    			"Points": [
    				{
    					"T": 1575989859642,
    					"V": "hello, word!"
    				}
    			]
    		}
    	]
    }

#### TODO:
- [ ] add ping command