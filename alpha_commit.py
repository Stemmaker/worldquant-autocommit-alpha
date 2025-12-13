import json
import os
from datetime import datetime
from os.path import expanduser
from time import sleep

import pandas as pd
import ast
import requests
from requests.auth import HTTPBasicAuth



class BrainAPIClient:
    API_BASE_URL = 'https://api.worldquantbrain.com'

    def __init__(self, credentials_file='brain_credentials_copy.txt'):
        """初始化 API 客户端"""

        self.session = requests.Session()
        self._setup_authentication(credentials_file)

    def _setup_authentication(self, credentials_file):
        """设置认证"""

        try:
            with open(expanduser(credentials_file)) as f:
                credentials = json.load(f)
            username, password = credentials
            self.session.auth = HTTPBasicAuth(username, password)

            response = self.session.post(f"{self.API_BASE_URL}/authentication")
            if response.status_code not in [200, 201]:
                raise Exception(f"认证失败: HTTP {response.status_code}")

            print("✅ 认证成功!")

        except Exception as e:
            print(f"❌ 认证错误: {str(e)}")
            raise

    def submit_alpha(self, alpha_id):
        """提交单个 Alpha"""

        submit_url = f"{self.API_BASE_URL}/alphas/{alpha_id}/submit"

        for attempt in range(5):
            print(f"🔄 第 {attempt + 1} 次尝试提交 Alpha {alpha_id}")

            # POST 请求
            res = self.session.post(submit_url)
            if res.status_code == 201:
                print("✅ POST:等待提交完成...")
            elif res.status_code in [400, 403]:
                print(f"❌ 提交被拒绝 ({res.status_code})")
                return False
            else:
                sleep(3)
                continue

            # 检查提交状态
            while True:
                res = self.session.get(submit_url)
                retry = float(res.headers.get('Retry-After', 0))

                if retry == 0:
                    if res.status_code == 200:
                        print("✅ 提交成功!")
                        return True
                    else:
                        data = res.json()
                        checks = data.get('is', {}).get('checks', [])
                        check_results = {item.get('name'): item.get('value') for item in checks}
                        msg = (f"❌ 提交失败: SHARPE: PASS[{check_results.get('LOW_SHARPE')}], " \
                        f"FITNESS: PASS[{check_results.get('LOW_FITNESS')}], " \
                        f"TURNOVER: PASS[{check_results.get('HIGH_TURNOVER')}], " \
                        f"SUB_UNIVERSE_SHARPE: PASS[{check_results.get('LOW_SUB_UNIVERSE_SHARPE')}], " \
                        f"SELF_CORRELATION: FAIL[{check_results.get('SELF_CORRELATION')}]")
                        print(msg)
                        return False

                sleep(retry)

        return False

    def submit_multiple_alphas(self, alpha_ids):
        """批量提交 Alpha"""
        successful = []
        failed = []

        for alpha_id in alpha_ids:
            if self.submit_alpha(alpha_id):
                successful.append(alpha_id)
            else:
                failed.append(alpha_id)

            if alpha_id != alpha_ids[-1]:
                sleep(10)

        return successful, failed

def save_candidate_alpha_ids(simulated_alphas_file, candidate_alpha_id_file):
    """
    从模拟结果中提取合格的 Alpha ID 并保存到文件。
    
    筛选条件：
    每行数据中 'checks' 列表里，以下六项指标的 'result' 必须为 'PASS'：
    1. LOW_SHARPE
    2. LOW_FITNESS
    3. LOW_TURNOVER
    4. HIGH_TURNOVER
    5. CONCENTRATED_WEIGHT
    6. LOW_SUB_UNIVERSE_SHARPE
    
    忽略其他检查项（如 UNITS 警告等）。
    """
    
    # 定义需要强制检查通过的指标集合
    required_checks = {
        'LOW_SHARPE', 
        'LOW_FITNESS', 
        'LOW_TURNOVER', 
        'HIGH_TURNOVER', 
        'CONCENTRATED_WEIGHT', 
        'LOW_SUB_UNIVERSE_SHARPE'
    }
    
    valid_alpha_ids = []

    try:
        # 读取 CSV 文件，不带表头，以防表头格式不规范
        # 如果文件确实有标准表头，可以改为 header=0
        df = pd.read_csv(simulated_alphas_file, header=None)
        
        for _, row in df.iterrows():
            try:
                # 1. 提取 Alpha ID (第一列)
                alpha_id = str(row[0]).strip()
                
                # 2. 寻找包含 check 信息的字典列
                # 由于 CSV 格式可能变动，这里遍历该行所有列，寻找包含 'checks' 字段的字符串
                stats_str = None
                for col in row:
                    if isinstance(col, str) and "'checks':" in col:
                        stats_str = col
                        break
                
                if not stats_str:
                    continue

                # 3. 解析字符串为字典
                data_dict = ast.literal_eval(stats_str)
                checks_list = data_dict.get('checks', [])
                
                # 将该 Alpha 的所有检查结果转为 {name: result} 的字典映射，方便查询
                check_results = {item.get('name'): item.get('result') for item in checks_list}
                
                # 4. 验证指定的六项指标
                is_qualified = True
                for req_metric in required_checks:
                    # 如果某项关键指标的结果不是 'PASS' (或者是缺失)，则标记为不合格
                    # 注意：这里严格要求为 'PASS'。如果允许 'WARNING'，需修改此处逻辑。
                    if check_results.get(req_metric) != 'PASS':
                        is_qualified = False
                        break
                
                if is_qualified:
                    valid_alpha_ids.append(alpha_id)

            except Exception as e:
                # 如果某行解析出错（如格式损坏），跳过该行
                continue
        
        # 5. 将结果保存到 txt 文件
        with open(candidate_alpha_id_file, 'w', encoding='utf-8') as f:
            for aid in valid_alpha_ids:
                f.write(f"{aid}\n")
                
        print(f"处理完成：共找到 {len(valid_alpha_ids)} 个合格的 Alpha，已保存至 {candidate_alpha_id_file}")

    except FileNotFoundError:
        print(f"错误：找不到文件 {simulated_alphas_file}")
    except Exception as e:
        print(f"发生未知错误：{e}")


def submit_alpha_ids(alpha_id_path, num_to_submit=2):
    """提交保存的 Alpha ID"""
    brain = BrainAPIClient()
    try:
        if not os.path.exists(alpha_id_path):
            print("❌ 没有找到保存的Alpha ID文件")
            return

        with open(alpha_id_path, 'r') as f:
            alpha_ids = [line.strip() for line in f.readlines() if line.strip()]

        if not alpha_ids:
            print("❌ 没有可提交的Alpha ID")
            return

        print(f"\n📝 已保存的Alpha ID列表共 {len(alpha_ids)} 个")
        

        if num_to_submit > len(alpha_ids):
            num_to_submit = len(alpha_ids)
        successful, failed = [], []
        idx = 0
        while len(successful) < num_to_submit and idx < len(alpha_ids):  # 添加索引边界检查
            selected_ids = alpha_ids[idx: idx + (num_to_submit - len(successful))]
            if not selected_ids:  # 防御性检查
                break
            new_successful, new_failed = brain.submit_multiple_alphas(selected_ids)
            successful.extend(new_successful)
            failed.extend(new_failed)
            idx += len(selected_ids)

        if len(successful) < num_to_submit:
            print(f"⚠️ 警告: 仅成功提交 {len(successful)} 个,目标是 {num_to_submit} 个")
        else:
            print(f"✅ 成功提交 {len(successful)} 个 Alpha ID")
        
        # 更新 alpha_ids.txt
        successful_set = set(successful)
        failed_set = set(failed)
        remaining_ids = list(set(alpha_ids).difference(successful_set, failed_set))
        with open(alpha_id_path, 'w') as f:
            f.writelines([f"{id}\n" for id in remaining_ids])

    except Exception as e:
        print(f"❌ 提交 Alpha 时出错: {str(e)}")


def main():
    print("🚀 启动 WorldQuant Brain Alpha 提交系统")
    alpha_id_path = "alpha_ids/alpha_ids.txt"
    simulated_alphas_file = "simulated_alphas/simulated_alphas_2025-12-10.csv"
    print("\n📋 请选择操作:")
    print("1: 提取合格 Alpha ID 并保存")
    print("2: 提交已保存的合格 Alpha ID")
    print("3: 提取并提交合格 Alpha ID")
    choice = int(input("\n请选择操作 (1-3): "))
    match choice:
        case 1:
            save_candidate_alpha_ids(simulated_alphas_file, alpha_id_path)
        case 2:
            num_to_submit = int(input("请输入要提交的合格 Alpha 数量: "))
            submit_alpha_ids(alpha_id_path, num_to_submit=num_to_submit)
        case 3:
            save_candidate_alpha_ids(simulated_alphas_file, alpha_id_path)
            num_to_submit = int(input("请输入要提交的合格 Alpha 数量: "))
            submit_alpha_ids(alpha_id_path, num_to_submit=num_to_submit)
        case _:
            print("❌ 无效的选择")
            return
        
if __name__ == "__main__":
    main()
    