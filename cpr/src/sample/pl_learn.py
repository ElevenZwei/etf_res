import polars as pl
import datetime as dt

df = pl.DataFrame(
    {
        "name": ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
        "birthdate": [
            dt.date(1997, 1, 10),
            dt.date(1985, 2, 15),
            dt.date(1983, 3, 22),
            dt.date(1981, 4, 30),
        ],
        "weight": [57.9, 72.5, 53.6, 83.1],  # (kg)
        "height": [1.56, 1.77, 1.65, 1.75],  # (m)
    }
)
print(df)

result = df.group_by(
    (pl.col("birthdate").dt.year() // 10 * 10).alias("decade"),
    maintain_order=True,
).agg(
    pl.len().alias("sample_size"),  # 计算每个组的样本数量
    pl.col("weight").mean().round(2).alias("avg_weight"), # 计算平均体重并四舍五入到小数点后两位
    pl.col("height").max().alias("tallest"), # 计算最高身高
)
print(result)

result = (
    df.with_columns(
        (pl.col("birthdate").dt.year() // 10 * 10).alias("decade"),
        pl.col("name").str.split(by=" ").list.first(), # 提取名字的第一个部分，相当于 SQL 的 split_part(name, ' ', 1)
    )
    .select(
        pl.all().exclude("birthdate"), # 排除 birthdate 列
    )
    .group_by(
        pl.col("decade"),
        maintain_order=True,
    )
    .agg(
        pl.col("name"), # 收集每个组的名字列表，相当于 SQL 的 array_agg
        pl.col("weight", "height").mean().round(2).name.prefix("avg_"), # 计算平均体重和身高，并添加前缀 avg_
    )
)
print(result)
