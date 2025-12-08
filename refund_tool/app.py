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
# 从 Streamlit Cloud 后台 Secrets 中读取配置
DIFY_API_KEY = st.secrets["DIFY_API_KEY"]
BASE_URL = st.secrets["BASE_URL"]

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
        
        # 详细记录响应状态
        if response.status_code == 200:
            result_data = response.json()
            outputs = result_data.get('data', {}).get('outputs', {})
            return {
                "退款根因": outputs.get('root_cause', '未分类'), 
                "优化策略": outputs.get('strategy', '-'),
                "行动计划": outputs.get('action_plan', '-'),
                "状态": "成功",
                "错误详情": ""
            }
        else:
            # 尝试获取错误响应的详细信息
            try:
                error_data = response.json()
                error_msg = error_data.get('message', '未知错误')
            except:
                error_msg = response.text[:200] if response.text else '无响应内容'
            
            error_detail = f"HTTP {response.status_code}: {error_msg}"
            return {
                "状态": f"失败: {response.status_code}", 
                "退款根因": "API错误", 
                "优化策略": "-", 
                "行动计划": "-",
                "错误详情": error_detail
            }
    except requests.exceptions.Timeout:
        return {
            "状态": "超时", 
            "退款根因": "请求超时", 
            "优化策略": "-", 
            "行动计划": "-",
            "错误详情": "请求超过60秒未响应"
        }
    except requests.exceptions.ConnectionError:
        return {
            "状态": "连接失败", 
            "退款根因": "网络错误", 
            "优化策略": "-", 
            "行动计划": "-",
            "错误详情": "无法连接到API服务器"
        }
    except Exception as e:
        return {
            "状态": f"异常", 
            "退款根因": "请求异常", 
            "优化策略": "-", 
            "行动计划": "-",
            "错误详情": str(e)
        }

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
# 构造用户ID字符串,例如：ZhangSan-Operation
current_user = st.session_state.user_info
user_id_str = f"{current_user['name']}-{current_user['dept']}"

st.write(f"👤 当前用户: **{current_user['name']}** | 🏢 部门: **{current_user['dept']}**")
if st.button("退出登录"):
    st.session_state.user_info = {}
    st.rerun()
st.markdown("---")

uploaded_file = st.file_uploader("上传 Excel 文件 (.xlsx)", type=["xlsx", "csv"])

if uploaded_file:
    # 读取文件 - 修复编码问题
    try:
        if uploaded_file.name.endswith('.csv'):
            # 尝试多种常见编码格式
            try:
                df = pd.read_csv(uploaded_file, encoding='utf-8')
            except UnicodeDecodeError:
                uploaded_file.seek(0)  # 重置文件指针
                try:
                    df = pd.read_csv(uploaded_file, encoding='gbk')
                except UnicodeDecodeError:
                    uploaded_file.seek(0)
                    try:
                        df = pd.read_csv(uploaded_file, encoding='latin1')
                    except UnicodeDecodeError:
                        uploaded_file.seek(0)
                        df = pd.read_csv(uploaded_file, encoding='ISO-8859-1')
        else:
            df = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"❌ 文件读取失败: {str(e)}")
        st.info("💡 提示：如果是 CSV 文件，请尝试用 Excel 另存为 UTF-8 格式的 CSV，或者直接上传 Excel 文件（.xlsx）")
        st.stop()
    
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
        with c5: 
            # 优先匹配 customer-comments，其次 customer_comments
            comments_idx = get_idx('customer-comments')
            if comments_idx == 0 and 'customer-comments' not in cols:
                comments_idx = get_idx('customer_comments')
            c_comments = st.selectbox("评论列", cols, index=comments_idx)
        
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
        
        # 用于收集错误信息
        error_logs = []
        
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
                    
                    # 记录错误信息
                    if res['状态'] != "成功" and res.get('错误详情'):
                        error_logs.append({
                            '行号': index + 2,  # Excel行号(从2开始,因为有表头)
                            'SKU': str(result_df.at[index, column_map['sku']]),
                            '错误类型': res['退款根因'],
                            '错误详情': res['错误详情']
                        })
                except Exception as e:
                    result_df.at[index, 'AI-退款根因'] = "系统异常"
                    result_df.at[index, 'AI-优化策略'] = "-"
                    result_df.at[index, 'AI-行动计划'] = "-"
                    error_logs.append({
                        '行号': index + 2,
                        'SKU': str(result_df.at[index, column_map['sku']]),
                        '错误类型': '系统异常',
                        '错误详情': str(e)
                    })
                
                completed += 1
                progress_bar.progress(completed / total)
                status_text.text(f"正在处理: {completed}/{total}")

        st.balloons()
        
        # 显示处理统计
        success_count = len(result_df[result_df['AI-退款根因'].notna() & 
                                       ~result_df['AI-退款根因'].isin(['API错误', '请求超时', '网络错误', '请求异常', '系统异常'])])
        error_count = len(error_logs)
        
        if error_count == 0:
            st.success(f"✅ 处理完成！成功分析 {success_count}/{total} 条数据")
        else:
            st.warning(f"⚠️ 处理完成！成功: {success_count} 条，失败: {error_count} 条")
            
            # 显示错误详情
            with st.expander(f"📋 查看 {error_count} 条错误详情", expanded=True):
                error_df = pd.DataFrame(error_logs)
                st.dataframe(error_df, use_container_width=True)
                
                # 错误类型统计
                if len(error_df) > 0:
                    st.markdown("#### 错误类型分布")
                    error_type_counts = error_df['错误类型'].value_counts()
                    for error_type, count in error_type_counts.items():
                        st.write(f"- **{error_type}**: {count} 条")
                    
                    # 提供错误日志下载
                    error_buffer = io.BytesIO()
                    with pd.ExcelWriter(error_buffer, engine='xlsxwriter') as writer:
                        error_df.to_excel(writer, index=False, sheet_name='错误日志')
                    
                    st.download_button(
                        label="📥 下载错误日志",
                        data=error_buffer.getvalue(),
                        file_name=f"错误日志_{int(time.time())}.xlsx",
                        mime="application/vnd.ms-excel"
                    )
        
        # === 筛选并重命名列 ===
        # 保留指定的列
        output_columns = []
        column_rename = {}
        
        # 添加原始列
        if c_sku in result_df.columns:
            output_columns.append(c_sku)
            column_rename[c_sku] = 'sku'
        if c_asin in result_df.columns:
            output_columns.append(c_asin)
            column_rename[c_asin] = 'asin'
        if c_fnsku in result_df.columns:
            output_columns.append(c_fnsku)
            column_rename[c_fnsku] = 'fnsku'
        if c_reason in result_df.columns:
            output_columns.append(c_reason)
            column_rename[c_reason] = 'reason'
        if c_comments in result_df.columns:
            output_columns.append(c_comments)
            column_rename[c_comments] = 'customer-comments'
        
        # 添加AI生成的列
        if 'AI-退款根因' in result_df.columns:
            output_columns.append('AI-退款根因')
            column_rename['AI-退款根因'] = '退款根因'
        if 'AI-优化策略' in result_df.columns:
            output_columns.append('AI-优化策略')
            column_rename['AI-优化策略'] = '根因优化策略'
        if 'AI-行动计划' in result_df.columns:
            output_columns.append('AI-行动计划')
            column_rename['AI-行动计划'] = '行动计划'
        
        # 创建最终输出的DataFrame
        final_df = result_df[output_columns].copy()
        final_df = final_df.rename(columns=column_rename)
        
        # === 可视化看板 ===
        st.markdown("---")
        st.subheader("📊 分析结果看板")
        
        if '退款根因' in final_df.columns:
            counts = final_df['退款根因'].value_counts().reset_index()
            counts.columns = ['根因', '数量']
            # 按数量降序排序，水平条形图需要ascending=True使最高值在顶部
            counts = counts.sort_values(by='数量', ascending=True)
            fig = px.bar(counts, x='数量', y='根因', orientation='h', title="退货原因分析", 
                        text='数量', color_discrete_sequence=['#FF7F50'])
            # 设置文字竖直显示
            fig.update_traces(textangle=0, textposition='outside')
            st.plotly_chart(fig, use_container_width=True)
            
        if 'sku' in final_df.columns:
            sku_counts = final_df['sku'].value_counts().head(10).reset_index()
            sku_counts.columns = ['SKU', '退货次数']
            # 按退货次数降序排序，水平条形图需要ascending=True使最高值在顶部
            sku_counts = sku_counts.sort_values(by='退货次数', ascending=True)
            fig2 = px.bar(sku_counts, x='退货次数', y='SKU', orientation='h', title="退货产品TOP 10", 
                         text='退货次数', color_discrete_sequence=['#1E90FF'])
            # 设置文字竖直显示
            fig2.update_traces(textangle=0, textposition='outside')
            st.plotly_chart(fig2, use_container_width=True)

        # === 下载 ===
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False)
            
        st.download_button(
            label="📥 下载完整分析报告",
            data=buffer.getvalue(),
            file_name=f"分析报告_{int(time.time())}.xlsx",
            mime="application/vnd.ms-excel"
        )
