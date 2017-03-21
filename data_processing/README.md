# 数据处理
> 一些处理数据的脚本
### 数据导入脚本
> 需求是从特定目录下读取文件转换格式后导入到MongoDB
> 可源文件既不是CSV,又不是json,而且单个文件又比较大(1~5G);所以就弄个脚本，多进程导入
1. [import_data_from_file_to_mongodb.py](./import_data_from_file_to_mongodb.py)
