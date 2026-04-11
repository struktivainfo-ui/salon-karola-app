{% extends 'base.html' %}
{% block title %}Kundensuche - Salon Karola{% endblock %}
{% block content %}
<section class="app-hero">
  <div class="app-hero-copy">
    <h1 class="app-hero-title">Kundensuche</h1>
  </div>
  <div class="hero-actions-grid">
    <a class="hero-action primary" href="{{ url_for('customer_new') }}">Neuer Kontakt</a>
    <a class="hero-action" href="{{ url_for('calendar_view') }}">Kalender</a>
  </div>
</section>

<section class="card clean-card space-top">
  <form method="get" class="grid grid-2 customer-search-form">
    <div class="form-group">
      <label>Suche</label>
      <input type="text" name="q" value="{{ q }}" placeholder="Nachname, Vorname, Mail, Stadt, Telefon" autocomplete="off">
    </div>
    <div class="form-group">
      <label>Tag</label>
      <select name="tag">
        <option value="">Alle</option>
        {% for tag_row in tags %}
        <option value="{{ tag_row['tag'] }}" {% if tag == tag_row['tag'] %}selected{% endif %}>{{ tag_row['tag'] }} ({{ tag_row['cnt'] }})</option>
        {% endfor %}
      </select>
    </div>
    <div class="form-group">
      <label>Sortierung</label>
      <select name="sort">
        <option value="az" {% if sort == 'az' %}selected{% endif %}>Nachname A–Z</option>
        <option value="za" {% if sort == 'za' %}selected{% endif %}>Nachname Z–A</option>
        <option value="recent" {% if sort == 'recent' %}selected{% endif %}>Zuletzt mit Termin</option>
      </select>
    </div>
    <div class="actions customer-search-actions">
      <button type="submit">Suchen</button>
      <a class="btn btn-secondary" href="{{ url_for('customer_search_page') }}">Zurücksetzen</a>
    </div>
  </form>
</section>

<section class="card clean-card space-top">
  <div class="panel-header">
    <div><h2>Ergebnisse</h2></div>
    <span class="badge">{{ customers|length }}</span>
  </div>
  <div class="desktop-only">
    <div class="table-wrap">
      <table>
        <thead>
          <tr><th>Name</th><th>E-Mail</th><th>Mobil</th><th>Stadt</th></tr>
        </thead>
        <tbody>
          {% for customer in customers %}
          <tr>
            <td><a href="{{ url_for('customer_detail', customer_id=customer['_id']) }}">{{ customer['_name'] }}{% if customer['_firstname'] %}, {{ customer['_firstname'] }}{% endif %}</a></td>
            <td>{{ customer['_mail'] or '-' }}</td>
            <td>{{ customer['Customer_Mobiltelefon'] or customer['Customer_PersönlichesTelefon'] or '-' }}</td>
            <td>{{ customer['Customer_Stadt'] or '-' }}</td>
          </tr>
          {% else %}
          <tr><td colspan="4">Keine Kontakte gefunden.</td></tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>
  <div class="mobile-only">
    {% for customer in customers %}
    <a class="mobile-card link-card" href="{{ url_for('customer_detail', customer_id=customer['_id']) }}">
      <div class="row-between wrap-mobile">
        <h3>{{ customer['_name'] }}{% if customer['_firstname'] %}, {{ customer['_firstname'] }}{% endif %}</h3>
        <span class="badge badge-health-{{ customer_activity_status(customer['last_appointment_at']) }}">{{ customer_activity_status(customer['last_appointment_at']) }}</span>
      </div>
      <div class="mobile-line"><span class="mobile-label">E-Mail:</span> {{ customer['_mail'] or '-' }}</div>
      <div class="mobile-line"><span class="mobile-label">Telefon:</span> {{ customer['Customer_Mobiltelefon'] or customer['Customer_PersönlichesTelefon'] or '-' }}</div>
      <div class="mobile-line"><span class="mobile-label">Stadt:</span> {{ customer['Customer_Stadt'] or '-' }}</div>
    </a>
    {% else %}
    <div class="mobile-card">Keine Kontakte gefunden.</div>
    {% endfor %}
  </div>
</section>
{% endblock %}
