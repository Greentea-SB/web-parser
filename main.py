def extract_pnl_values(text):
    logger.info(f"Raw PnL text: {text}")
    values = ['N/A'] * 7

    try:
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        logger.info(f"Split lines: {lines}")

        # Улучшенное извлечение TXs
        for i, line in enumerate(lines):
            if '7D TXs' in line and i + 2 < len(lines):
                tx_values = [l for l in lines[i:i+4] if l.isdigit()]
                if len(tx_values) >= 2:
                    values[0] = tx_values[0]
                    values[1] = tx_values[1]
                break

        # Улучшенное извлечение Total PnL
        for i, line in enumerate(lines):
            if 'Total PnL' in line and i + 1 < len(lines):
                pnl_line = lines[i + 1]
                amount_match = re.search(r'[\+\-]?\$?([\d,.]+[KMB]?)', pnl_line)
                percent_match = re.search(r'\(([-\+]?\d+\.?\d*)%\)', pnl_line)
                
                if amount_match:
                    values[2] = amount_match.group(1)
                if percent_match:
                    values[3] = percent_match.group(1) + '%'

        # Улучшенное извлечение остальных значений
        label_mapping = {
            'Unrealized Profits': 4,
            '7D Avg Duration': 5,
            '7D Total Cost': 6
        }

        for i, line in enumerate(lines):
            for label, index in label_mapping.items():
                if label in line and i + 1 < len(lines):
                    value = lines[i + 1].replace('$', '').replace('+', '')
                    values[index] = value
                    break

        return values

    except Exception as e:
        logger.error(f"Error parsing PnL block: {e}")
        return values
