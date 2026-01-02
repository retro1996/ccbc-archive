PROJECT_NAME = "test" #比赛的显示名称，例如ccbc 16

MYSQL_CONST = {
    'host': 'localhost',
    'port': 3306,
    'user': 'archiver',
    'pass': 'password',
    'db': 'ccxc_prod',
}

BASE_DIR = f"D:\\Projects\\PuzzleHuntArchive\\public\\{PROJECT_NAME}"
STATIC_URL_PREFIX = "https://static.example.com/images/"
FINAL_PUZZLE_PID = 63 #Final meta pid
TOTAL_GAMETIME = 264.00 #未完赛显示的用时
CONFIG_START_TIME = 1766246400000 // 1000 #比赛开始的时间戳，在system_options表里的StartTime,记得除以1000

def map_pgid(id):
    const_mapping = {
        1: 'a',
        2: 'b', 
        3: 'c',
        4: 'd',
    }
    if id in const_mapping.keys():
        return const_mapping[id]
    else: return f'pg{id}'
