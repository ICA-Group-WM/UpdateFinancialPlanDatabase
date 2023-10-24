from datetime import datetime
from flask import Flask, request, jsonify
from decimal import Decimal
import stripe
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
stripe.api_key = os.environ.get('STRIPE_SECRET_KEY')

# This is your Stripe CLI webhook secret for testing your endpoint locally.
endpoint_secret = os.environ.get('STRIPE_ENDPOINT_SECRET')

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    print("Received webhook event")
    payload = request.data
    sig_header = request.headers.get('stripe-signature')
    event = None

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, endpoint_secret
        )
    except ValueError:
        return 'Invalid payload', 400
    except stripe.error.SignatureVerificationError:
        return 'Invalid signature', 400

    # Handle the event
    if event['type'] == 'payment_intent.succeeded':
        print(f"Received event type: {event['type']}")
        payment_intent = event.data['object']
        amount = payment_intent['amount'] / 100  # Convert to dollars from cents
        # If receipt_email is available in the payment_intent
        email = payment_intent.get('receipt_email')
        
        # If not, fetch from the associated customer (might incur additional API charges)
        if not email and payment_intent.get('customer'):
            customer = stripe.Customer.retrieve(payment_intent['customer'])
            email = customer.email
        update_database(amount, email)
        return jsonify(success=True), 200

    elif event['type'] == 'charge.refunded':
        charge = event.data['object']
        # Negative to represent money outflow
        amount = -charge['amount_refunded'] / 100  # Convert to dollars from cents
        
        # If receipt_email is available in the charge
        email = charge.get('receipt_email')
        
        # If not, fetch from the associated customer (might incur additional API charges)
        if not email and charge.get('customer'):
            customer = stripe.Customer.retrieve(charge['customer'])
            email = customer.email
        
        update_database(amount, email)
        return jsonify(success=True), 200
    
    return 'Unhandled event', 400

def update_database(amount, email):
    # Connect to your PostgreSQL database and retrieve individual database components from environment variables
    dbname = os.environ.get('DB_NAME')
    user = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    host = os.environ.get('DB_HOST')
    port = os.environ.get('DB_PORT')

    # Use the components to connect
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    cur = conn.cursor()

    # Convert the amount to Decimal
    amount = Decimal(str(amount))

    print(f"Received email: {email}")
    print(f"Received amount: {amount}")
    if amount < 0:  # If a negative amount, process as a refund
        refund_amount = abs(amount)
        print(f"Initiating refund of amount: {refund_amount}")
        
        # First, handle overflow payments
        cur.execute("""SELECT id, total_payment_amount_received
                    FROM clients 
                    WHERE email = %s AND payment_choice = 'Overflow payment'
                    ORDER BY payment_start_date DESC, id DESC""", (email,))

        overflow_records = cur.fetchall()
        refund_processed = False  # Adding a flag to check if refund is processed in overflow

        for record in overflow_records:
            client_id, total_paid = record
            
            if refund_amount <= total_paid:
                if refund_amount == total_paid:
                    print(f"Full refund processed in overflow: deleting record {client_id}")
                    cur.execute("""DELETE FROM clients WHERE id = %s""", (client_id,))
                    cur.execute("""DELETE FROM client_advisor_association WHERE client_id = %s""", (client_id,))
                else:
                    print(f"Partial refund processed in overflow: updating record {client_id} to {total_paid - refund_amount}")
                    cur.execute("""UPDATE clients 
                                SET total_payment_amount_received = total_payment_amount_received - %s 
                                WHERE id = %s""", (str(refund_amount), client_id))
                
                conn.commit()
                refund_processed = True  # Set flag to True once refund is processed
                break  
            else:
                print(f"Reducing refund amount by {total_paid}. New refund amount: {refund_amount - total_paid}")
                refund_amount -= total_paid
                cur.execute("""DELETE FROM clients WHERE id = %s""", (client_id,))
                cur.execute("""DELETE FROM client_advisor_association WHERE client_id = %s""", (client_id,))
                conn.commit()

        # Handling non-overflow payments if there's still refund_amount left and it wasn't processed in overflow
        if not refund_processed and refund_amount > 0:
            print("Handling non-overflow payments")
            cur.execute("""SELECT id, total_payment_amount_received
                        FROM clients 
                        WHERE email = %s AND payment_choice <> 'Overflow payment'
                        ORDER BY payment_start_date DESC, id DESC""", (email,))
            
            other_records = cur.fetchall()
            
            for record in other_records:
                client_id, total_paid = record
                
                if refund_amount <= total_paid:
                    print(f"Refunding {refund_amount} from non-overflow record {client_id}")
                    cur.execute("""UPDATE clients 
                                SET total_payment_amount_received = total_payment_amount_received - %s 
                                WHERE id = %s""", (str(refund_amount), client_id))
                    conn.commit()
                    break  # Exit loop once refund is processed
                else:
                    refund_amount -= total_paid
                    cur.execute("""UPDATE clients 
                                SET total_payment_amount_received = 0
                                WHERE id = %s""", (client_id,))
                    conn.commit()
            
            if refund_amount > 0:
                print("Error: You cannot refund more money than you have paid")

    else:
        # Get all records for the client sorted by payment_start_date and then by ID
        select_sql = """SELECT id, total_billing_amount, total_payment_amount_received
                    FROM clients 
                    WHERE email = %s
                    ORDER BY payment_start_date ASC, id ASC"""
        cur.execute(select_sql, (email,))
        records_to_update = cur.fetchall()

        # Process each record for the client
        while amount > 0 and records_to_update:
            record = records_to_update.pop(0)
            client_id, total_billing, total_paid = record

            # Calculate the payment to apply
            payment_to_apply = min(total_billing - total_paid, amount)

            # Update the total_payment_amount_received for the client
            update_sql = sql.SQL(
                "UPDATE clients SET total_payment_amount_received = total_payment_amount_received + %s WHERE id = %s"
            )
            cur.execute(update_sql, (str(payment_to_apply), client_id))
            conn.commit()

            # Deduct the applied payment from the amount
            amount -= payment_to_apply

        # When amount is still left after all records are updated, handle overflow
        if amount > 0:
            print(f"Handling overflow for amount: {amount}")

            # Check for existing overflow records for the client
            select_existing_overflow_sql = """SELECT id, total_payment_amount_received 
                                            FROM clients 
                                            WHERE email = %s AND payment_choice = 'Overflow payment' 
                                            ORDER BY id DESC 
                                            LIMIT 1"""
            cur.execute(select_existing_overflow_sql, (email,))
            existing_overflow = cur.fetchone()

            if existing_overflow:
                # Existing overflow found. Update it.
                overflow_id, existing_amount = existing_overflow
                print(f"Existing overflow record found (id: {overflow_id}, amount: {existing_amount}). Updating...")

                update_sql = """
                    UPDATE clients 
                    SET total_payment_amount_received = total_payment_amount_received + %s 
                    WHERE id = %s
                """
                cur.execute(update_sql, (str(amount), overflow_id))
                conn.commit()

                print(f"Updated overflow record {overflow_id} with new amount: {existing_amount + amount}")
            else:
                # No existing overflow found. Create new overflow record.
                print("No existing overflow record found. Creating a new one...")

                select_latest_client_details_sql = """SELECT id, client_name_first, client_name_last 
                                                FROM clients 
                                                WHERE email = %s 
                                                ORDER BY id DESC 
                                                LIMIT 1"""
                cur.execute(select_latest_client_details_sql, (email,))
                client_details = cur.fetchone()

                if client_details:
                    original_client_id, client_name_first, client_name_last = client_details
                else:
                    original_client_id, client_name_first, client_name_last = None, 'Unknown', 'Unknown'

                insert_sql = """
                    INSERT INTO clients (client_name_first, client_name_last, payment_choice, 
                        total_billing_amount, num_billing_periods, payment_start_date, payment_end_date,
                        total_billing_per_period, paying_by_check, confirmation_id, date_check_mailed, 
                        check_image, date_check_received, advisor_id, created_at, total_payment_amount_received, email)
                    VALUES (%s, %s, %s, %s, NULL, NULL, NULL, NULL, FALSE, NULL, NULL, NULL, NULL, NULL, %s, %s, %s)
                    RETURNING id
                """

                current_date = datetime.now()
                cur.execute(insert_sql, (client_name_first, client_name_last, "Overflow payment", 
                                        Decimal('0.00'), current_date, amount, email))
                new_client_id = cur.fetchone()[0]
                conn.commit()

                print(f"Inserted new overflow record for {email} with amount: {amount}")

                if original_client_id:
                    select_advisor_association_sql = """SELECT advisor_id FROM client_advisor_association WHERE client_id = %s"""
                    cur.execute(select_advisor_association_sql, (original_client_id,))
                    advisor_ids = [row[0] for row in cur.fetchall()]

                    insert_association_sql = """INSERT INTO client_advisor_association (client_id, advisor_id) VALUES (%s, %s)"""
                    for advisor_id in advisor_ids:
                        cur.execute(insert_association_sql, (new_client_id, advisor_id))
                        conn.commit()


    cur.close()
    conn.close()

if __name__ == '__main__':
    app.run(port=4242, debug=True)
