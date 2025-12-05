import streamlit as st
import pandas as pd
import requests
import concurrent.futures
import time
import plotly.express as px
import io

# ================= 页面配置 =================
st.set_page_config(page_title="亚马逊退款智能分析", layout="wide", page_icon="📊")

# ================= 1. 获取云端密钥 (Secrets) =================
# 这里的代码会自动去读取 Streamlit Cloud 后台配置的密钥
# 如果你在本地运行报错，请确保你配置了 .streamlit/secrets.toml 或者临时把这里改成明文
try:
    DIFY_API_KEY = st.secrets["DIFY_API_KEY"]
    BASE_URL = st.secrets["BASE_URL"]
except:
    st.error("❌ 未检测到密钥配置！请在 Streamlit Cloud 的 Secrets 中配置 DIFY_API_KEY 和 BASE_URL。")
    st.stop()

# ================= 2. 核心处理逻辑 =================
def analyze_single_row(row, column_map, user_identifier):
    headers = {
        "Authorization": f"Bearer {DIFY_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # 构造 Dify 输入变量
    inputs = {
        "sku": str(row.get(column_map['sku'], '')),
        "asin": str(row.get(column_map['asin'], '')),
        "fnsku": str(row.get(column_map['fnsku'], '')),
        "reason": str(row.get(column_map['reason'], '')),
        "comments": str(row.get(column_map['comments'], ''))
    }
    
    # 【关键点】将用户信息传给 Dify 的 user 字段
    payload = {
        "inputs": inputs,
        "response_mode": "blocking",
        "user": user_identifier  # 这里传入 "张三-运营部"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/workflows/run", json=payload, headers=headers, timeout=60)
        if response.status_code == 200:
            result_data = response.json()
            outputs = result_data.get('data', {}).get('outputs', {})
            return {
                "退款根因": outputs.get('root_cause', '未分类'), 
                "优化策略": outputs.get('strategy', '-'),
                "行动计划": outputs.get('action_plan', '-'),
                "状态": "成功"
            }
        else:
            return {"状态": f"失败: {response.status_code}", "退款根因": "API错误", "优化策略": "-", "行动计划": "-"}
    except Exception as e:
        return {"状态": f"错误: {str(e)}", "退款根因": "请求异常", "优化策略": "-", "行动计划": "-"}

# ================= 3. 用户登录界面 =================
if 'user_info' not in st.session_state:
    st.session_state.user_info = {}

if not st.session_state.user_info.get('logged_in'):
    st.markdown("## 👋 欢迎使用退款分析工具")
    st.info("请输入您的信息以开始使用（记录将同步至后台日志）")
    
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("您的姓名")
    with col2:
        dept = st.text_input("所属部门")
        
    if st.button("进入系统", type="primary"):
        if name and dept:
            st.session_state.user_info = {'name': name, 'dept': dept, 'logged_in': True}
            st.rerun()
        else:
            st.warning("请完整填写姓名和部门")
    st.stop()

# ================= 4. 主工作台 =================
# 构造用户ID字符串，例如：ZhangSan-Operation
current_user = st.session_state.user_info
user_id_str = f"{current_user['name']}-{current_user['dept']}"

st.write(f"👤 当前用户: **{current_user['name']}** | 🏢 部门: **{current_user['dept']}**")
if st.button("退出登录"):
    st.session_state.user_info = {}
    st.rerun()
st.markdown("---")

uploaded_file = st.file_uploader("上传 Excel 文件 (.xlsx)", type=["xlsx", "csv"])

if uploaded_file:
    # 读取文件
    if uploaded_file.name.endswith('.csv'):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)
    
    st.success(f"✅ 成功加载 {len(df)} 条数据")

    # 字段映射
    with st.expander("配置数据列对应关系", expanded=True):
        cols = df.columns.tolist()
        c1, c2, c3, c4, c5 = st.columns(5)
        # 辅助函数：自动查找列名
        def get_idx(k): return cols.index(k) if k in cols else 0
        
        with c1: c_sku = st.selectbox("SKU列", cols, index=get_idx('sku'))
        with c2: c_asin = st.selectbox("ASIN列", cols, index=get_idx('asin'))
        with c3: c_fnsku = st.selectbox("FNSKU列", cols, index=get_idx('fnsku'))
        with c4: c_reason = st.selectbox("原因列", cols, index=get_idx('reason'))
        with c5: c_comments = st.selectbox("评论列", cols, index=get_idx('customer_comments'))
        
        column_map = {'sku': c_sku, 'asin': c_asin, 'fnsku': c_fnsku, 'reason': c_reason, 'comments': c_comments}

    # 运行按钮
    st.subheader("🚀 批量分析")
    max_workers = st.slider("并发速度", 1, 20, 10)
    
    if st.button("开始运行", type="primary"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 复制一份数据用于写入结果
        result_df = df.copy()
        total = len(df)
        completed = 0
        
        start_time = time.time()
        
        # 线程池并发调用
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {
                # 注意：这里把 user_id_str 传进去了
                executor.submit(analyze_single_row, row, column_map, user_id_str): index 
                for index, row in result_df.iterrows()
            }
            
            for future in concurrent.futures.as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    res = future.result()
                    result_df.at[index, 'AI-退款根因'] = res['退款根因']
                    result_df.at[index, 'AI-优化策略'] = res['优化策略']
                    result_df.at[index, 'AI-行动计划'] = res['行动计划']
                except:
                    result_df.at[index, 'AI-退款根因'] = "失败"
                
                completed += 1
                progress_bar.progress(completed / total)
                status_text.text(f"正在处理: {completed}/{total}")

        st.balloons()
        st.success("处理完成！请查看下方图表或下载报告。")
        
        # === 可视化看板 ===
        st.markdown("---")
        st.subheader("📊 分析结果看板")
        v1, v2 = st.columns(2)
        
        with v1:
            if 'AI-退款根因' in result_df.columns:
                counts = result_df['AI-退款根因'].value_counts().reset_index()
                counts.columns = ['根因', '数量']
                # 按数量降序排序
                counts = counts.sort_values(by='数量', ascending=True)
                fig = px.bar(counts, x='数量', y='根因', orientation='h', title="退货原因分析", text_auto=True, color_discrete_sequence=['#FF7F50'])
                st.plotly_chart(fig, use_container_width=True)
                
        with v2:
            if c_sku in result_df.columns:
                sku_counts = result_df[c_sku].value_counts().head(10).reset_index()
                sku_counts.columns = ['SKU', '退货次数']
                # 按退货次数降序排序
                sku_counts = sku_counts.sort_values(by='退货次数', ascending=True)
                fig2 = px.bar(sku_counts, x='退货次数', y='SKU', orientation='h', title="退货产品TOP 10", text_auto=True, color_discrete_sequence=['#1E90FF'])
                st.plotly_chart(fig2, use_container_width=True)

        # === 下载 ===
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            result_df.to_excel(writer, index=False)
            
        st.download_button(
            label="📥 下载完整分析报告",
            data=buffer.getvalue(),
            file_name=f"分析报告_{int(time.time())}.xlsx",
            mime="application/vnd.ms-excel"
        )
