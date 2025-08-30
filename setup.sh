mkdir -p~/.streamlit/

echo "\
[server]\n\
port = $PORT\n\
enableCORS = fasle\n\
headless = true\n\
\n\
" > ~/.streamlit/config.toml