# 20 个学生，10 个男孩和 10 个女孩，他们坐在同一排的 20 个座位上，
# 任意 3 个男孩不相邻，任意 3 个女孩也不相邻的概率是多少？

def count_arrangements(n, b_max):
    # 初始化 DP 数组
    B = [[0] * (b_max + 1) for _ in range(n + 1)]  # 以男孩结尾
    G = [[0] * (b_max + 1) for _ in range(n + 1)]  # 以女孩结尾
    
    # 初始条件
    B[0][0] = 1
    G[0][0] = 1
    B[1][1] = 1  # 第一个座位是男孩
    G[1][0] = 1  # 第一个座位是女孩

    # 状态转移
    for i in range(2, n + 1):  # 遍历座位数
        for b in range(b_max + 1):  # 遍历男孩数量
            # 更新 B[i][b]
            if b > 0:
                B[i][b] = B[i - 1][b - 1] + G[i - 1][b - 1]  # 男孩或女孩转移
                if i >= 3 and b >= 3:
                    B[i][b] -= G[i - 3][b - 3]  # 去掉以 GGG 结尾的不合法情况

            # 更新 G[i][b]
            if i - b > 0:
                G[i][b] = G[i - 1][b] + B[i - 1][b]  # 男孩或女孩转移
                if i >= 3 and i - b >= 3:
                    G[i][b] -= B[i - 3][b]  # 去掉以 BBB 结尾的不合法情况

    # 总数为最后一排以男孩或女孩结尾的合法分配数之和
    total_valid = B[n][b_max] + G[n][b_max]
    return total_valid

from itertools import permutations

def validate_arrangement(arrangement):
    """检查一个分配是否满足连续性约束"""
    for i in range(len(arrangement) - 2):
        if arrangement[i] == arrangement[i+1] == arrangement[i+2]:
            return False
    return True

def count_valid_arrangements(person, boys):
    # 所有可能的排列
    total = 0
    valid = 0
    boys_and_girls = 'B' * boys + 'G' * (person - boys)
    print(boys_and_girls)
    for arrangement in set(permutations(boys_and_girls)):
        total += 1
        if validate_arrangement(arrangement):
            valid += 1
    return valid, total

# print(count_arrangements(20, 10))
# print(count_valid_arrangements(10, 5))


# 把 N 个元素的集合划分成 K 的集合的划分方法。
def stirling_number_dp(n, k):
    # 初始化 dp 表
    dp = [[0] * (k + 1) for _ in range(n + 1)]
    dp[0][0] = 1  # S(0, 0) = 1
    
    for i in range(1, n + 1):
        for j in range(1, k + 1):
            dp[i][j] = j * dp[i - 1][j] + dp[i - 1][j - 1]
    
    return dp[n][k]

print(stirling_number_dp(3, 3))

