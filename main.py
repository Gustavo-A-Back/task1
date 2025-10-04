from utils import *
import pytz

st.markdown("""
    <style>
        /* Remove margens laterais padrão do Streamlit */
        .block-container {
            padding-left: 0rem;
            padding-right: 13rem;
            padding-top: 3rem;
            padding-bottom: 0rem;
            max-width: 95%; /* aumenta área útil horizontal */
        }

        /* Reduz largura da sidebar */
        section[data-testid="stSidebar"] {
            width: 260px !important; /* padrão ~340px */
        }

        /* Centraliza o conteúdo à esquerda */
        div[data-testid="stVerticalBlock"] {
            align-items: flex-start;
        }
        
    </style>
""", unsafe_allow_html=True)



tz_options = {
    "Brazil (Brasilia)": "America/Sao_Paulo",
    "Portugal (Lisbon)": "Europe/Lisbon",
    "Canada - Eastern (Toronto)": "America/Toronto",
    "Canada - Central (Winnipeg)": "America/Winnipeg",
    "Canada - Mountain (Edmonton)": "America/Edmonton",
    "Canada - Pacific (Vancouver)": "America/Vancouver",
    "UTC (Universal Coordinated Time)": "UTC"
}


#login

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "username" not in st.session_state:
    st.session_state.logged_in = ""

if not st.session_state.logged_in:
    st.title("Sign-in")

    with st.form("login_form"):
        st.subheader("Please enter your credentials")
        username = st.text_input("User")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")

    if submit:
        if authenticator(username, password):
            st.session_state.logged_in = True
            st.session_state.username = username
            st.success(f"Welcome, {username}!")
            st.rerun()  # 🔹 força recarregar a página e cair no modo “logado”
        else:
            st.warning("Access Denied")


else:

    #ajust timezone
    tz_name = st.sidebar.selectbox("Escolha o fuso horário:", list(tz_options.keys()))
    tz = pytz.timezone(tz_options[tz_name])

    #call func
    volumedata= get_volume_30m()
    data=dataframe(volumedata)

    #define date params
    data['timestamp'] = pd.to_datetime(data['timestamp'],utc=True)
    data = data.sort_values('timestamp')

    data["timestamp_local"] = data["timestamp"].dt.tz_convert(tz)
    data_min=data["timestamp_local"].min()
    data_max=data["timestamp_local"].max()

    #datetime
    start_time , endtime = st.select_slider(label="Select a Range",options=data["timestamp_local"],value=(data_min,data_max))
    datetime_selected_filter=data[(data["timestamp"]>=start_time) & (data["timestamp"] <= endtime)].copy() #dataframe filtered - datetime

    #pairs
    available_pairs=datetime_selected_filter["pair"].unique()
    pair_selected = st.sidebar.selectbox("Select a pair",options=available_pairs, index=3)
    pairs_selected_filter = datetime_selected_filter[datetime_selected_filter["pair"] == pair_selected].copy() #dataframe filtered - datetime


    #exchanges
    available_exchanges=pairs_selected_filter["exchange"].unique()
    exchanges_list = st.sidebar.multiselect("Select one or more exchanges",options=available_exchanges,default=[])
    exchanges_selected_filter = pairs_selected_filter[pairs_selected_filter["exchange"].isin(exchanges_list)].copy()


    #alive
    if exchanges_selected_filter.empty:
        st.warning("No data available for the selected filter combination. Try changing your selections.")

    else:
        base = alt.Chart(exchanges_selected_filter).mark_line(point=True).encode(
            x=alt.X("timestamp:T", title="Date", axis=alt.Axis(format="%d-%m %H:%M")),
            y=alt.Y("volume:Q", title="Volume", axis=alt.Axis(format=",.2f")),
            color=alt.Color("exchange:N", title="Exchange"),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Time",format="%d/%m/%Y %H:%M"),
                alt.Tooltip("exchange:N", title="Exchange"),
                alt.Tooltip("pair:N", title="Pair"),
                alt.Tooltip("volume:Q", title="Volume", format=",.2f")
            ]
        ).properties(width="container", height=420).interactive()


        chart = base.facet(
            column=alt.Column("pair:N", title=None,
                              header=alt.Header(labelOrient="bottom", labelAngle=0))
            ,
            columns=3  # ajuste quantas colunas quer por linha
        ).resolve_scale(y='independent')  # cada par pode ter escala de volume própria

        st.altair_chart(chart, use_container_width=True)
