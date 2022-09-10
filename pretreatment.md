- **author  : Shuxin_Wang**
- **email   : 213202122@seu.edu.cn**
- **time    : 2022/9/7**
- **version: 0.1.0**



# 预处理JSON文件约定

- `xxx.csv → xxx.json`

```json
{
    "isCompleted": true,
    
    "row": "$行数",
    
    "col": "$列数",
    
    "data":[
        {
            "canSearch": true,
            "column":[
                {
                    "isNo": false,
                    "value": "$源值",
                    "correction": [
                    "$拼写纠错结果",
                    "..."
                    ],
                    "QIDs":[
                        "$Qxx1",
                        "..."
                    ],
                    "Labels":[
                        "$搜索label1",
                        "..."
                    ],
                    "IRIs":[
                        "$IRI1",
                        "..."
                    ],
                    "Types":[
                        "$DBpedia<Type>1",
                        "..."
                    ]
                },

            ],
            "type": "$列数据类型(字符串string、数值型number、日期datetime、不可搜索:n)"
        },
        
    ]
}

```

- `csv_data[i][j] → json_data["data"][j]["column"][i]["value"]`



# 预处理过程



## 初始化设置

