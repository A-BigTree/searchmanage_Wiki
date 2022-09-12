- **author  : Shuxin_Wang**
- **email   : 213202122@seu.edu.cn**
- **time    : 2022/9/7**
- **version: 0.1.0**



# 模块视图

```mermaid
classDiagram

CSVPretreatment..|>JsonDataManage:继承
CSVPretreatment..>searchmanage:依赖


class JsonDataManage{
+josn_

+list:get_column_data(index:int, key:str)
+set_colum_data_data(data:Any, index:int, key:str)
+Any:get_colum_property(index:int, key:str)
+set_column_property(data:Any, index:int, key:str)
+Any:get_cell_data()
+set_cell_data()
}

class searchmanage{
<<packsge>>
}

class CSVPretreatment{
+csv_data
+search_index
+nlp
+relative_path
+csv_or_json_file
+file_type

+init_json_process()
+correct_process()
+search_process()
}
```

# 预处理JSON文件约定

- `xxx.csv → xxx.json`

```json
{
    "isCompleted": true,
    
    "row": "$行数",
    
    "col": "$列数",
    
    "keyColumnIndex": 0,
    
    "columnsType": ["列类别1", "..."],
    
    "data":[
        //可查询列
        {
            "canSearch": true,
            "type": "$列数据类型",
            "PID":"Pxx",
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
                        "$label1",
                        "..."
                    ],
                    "IRIs":[
                        "$IRI1",
                        "..."
                    ],
                    "Description":[
                        "$Description1",
                        "..."
                    ],
                    "Types":[
                        "$DBpedia<Type>1",
                        "..."
                    ],
                    //拓展信息，主题列含有
                    "Expansion": {
                        //属性筛选主题列拥有
                        "Properties":[
                            "..."
                        ],
                        //平均相似度，“Similarity”列表中的元素取平均
                        "AvgSimilarity":[
                            0.970,
                            0.656,
                        ],
                        //假设表格有3列，主题列与主题列之间相似度为0
                        "Similarity":[
                            [0, 0.975, 0.965],
                            [0, 0.656, 0.656],
                            "..."
                        ],
                        "PIDSimilarity":[
                            ["None", "P50", "P51"],
                            ["None", "P36", "P70"],
                            "..."
                        ],
                        //有的值匹配是非实体类型，因此为"None"
                        "QIDSimilarity":[
                            ["None", "Q60", "None"],
                            ["None", "Q5", "None"],
                            "..."
                        ],
                        "LabelSimilarity":[
                            ["None", "China", "1.46"],
                            ["None", "Human", "2.65"],
                            "..."
                        ]
                    },
                    //匹配实体ID，具体信息在不同任务中不同
                    "Target": Any
                },
            ]
        },
        //不可查询列
        {
            "canSearch": false,
            "PID":"Pxx",
            "column":[
                "$源值1", 
                "..."
                ]         
        }, 
    ]
}
```

- `json_data["data"][j]["canSearch"] is True`
  - `csv_data[i][j] → json_data["data"][j]["column"][i]["value"]`

- `json_data["data"][j]["canSearch"] is False`
  - `csv_data[i][j] → json_data["data"][j]["column"`][i]




# 预处理过程



## 初始化设置

- 以***<u>每列数据</u>***为基本单位；

1. 初步识别每列数据的数据类型，同时筛选可查询实体，即排除数字、日期时间等属性列，并同时找出主题列；
2. 对**可查询列**数据进行拼写纠错；
3. 对纠错完的结果进行`text→IRIs`查询；
3. 对主题列数据进行`Properties`查询
4. 保存json文件；





# <class>JsonDataManage</class>

> 对规定Json文件进行解析读取，修改值相关操作









# <class>CSVPretreatment</class>

> CSV文件预处理原子类







# <class>PretreatmentManage</class>

> CSV文件批处理管理类
