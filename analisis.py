import pandas as pd

# === Cargar los datos ===
flights = pd.read_csv('flights.csv')
bookings = pd.read_csv('bookings.csv')
users = pd.read_csv('users.csv')

print("Columnas en flights:", flights.columns)
print("Columnas en bookings:", bookings.columns)
print("Columnas en users:", users.columns)

# === Limpiar espacios en texto e IDs ===
flights = flights.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
bookings = bookings.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
users = users.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

flights['id'] = flights['id'].astype(str).str.strip()
bookings['flightId'] = bookings['flightId'].astype(str).str.strip()
bookings['userAppId'] = bookings['userAppId'].astype(str).str.strip()
users['id'] = users['id'].astype(str).str.strip()

# === Rellenar campos vacíos ===
bookings.fillna('Desconocido', inplace=True)
users.fillna('No indicado', inplace=True)

# === Convertir fechas y calcular duración de vuelos ===
flights['departureDate'] = pd.to_datetime(flights['departureDate'])
flights['arrivalDate'] = pd.to_datetime(flights['arrivalDate'])
flights['duration_mins'] = (flights['arrivalDate'] - flights['departureDate']).dt.total_seconds() / 60

# === Unir bookings con users y flights ===
bookings_users = bookings.merge(users, left_on='userAppId', right_on='id', suffixes=('', '_user'))
bookings_full = bookings_users.merge(flights, left_on='flightId', right_on='id', suffixes=('', '_flight'))

# === Reservas por usuario ===
reservas_por_usuario = bookings_full.groupby(['name', 'surname']) \
    .agg(total_reservas=('flightId', 'count'),
         destinos_mas_visitados=('destination', lambda x: x.mode()[0] + f" ({x.value_counts().iloc[0]} veces)" if not x.mode().empty else 'Ninguno')) \
    .reset_index()

# === Vuelos más reservados ===
vuelos_mas_reservados = bookings['flightId'].value_counts().reset_index()
vuelos_mas_reservados.columns = ['flightId', 'num_reservas']
vuelos_mas_reservados = vuelos_mas_reservados.merge(flights, left_on='flightId', right_on='id')

# === Guardar los resultados ===
flights.to_csv('flights_procesado.csv', index=False)
reservas_por_usuario.to_csv('reservas_por_usuario.csv', index=False)
vuelos_mas_reservados.to_csv('vuelos_mas_reservados.csv', index=False)

print("✅ Análisis finalizado correctamente.")
