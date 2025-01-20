import json
import xml.etree.ElementTree as ET
import csv
import logging
import re
from datetime import datetime
from dateutil import parser as date_parser
import string
import requests
import subprocess

import fitz  # PyMuPDF
from PIL import Image
import io
import os
import psycopg2
import string
import concurrent.futures
from datetime import datetime, date



logging.basicConfig(
    filename='API.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def establish_database_connection():
    try:
        conn = psycopg2.connect(database="xxx",
                                host="xxx",
                                user="xxx",
                                password="xxx",
                                port="5432")
        return conn
    except Exception as e:
        logging.error("Error connecting to the database: %s", str(e))
        raise
def get_metadata_by_con(doi):
    conn = establish_database_connection()
    cursor = conn.cursor()
    try:

        sql = """
        SELECT
        xxxx
        WHERE
            xxxxx = %s;"""
        cursor.execute(sql, (doi,))
        rows = cursor.fetchall()
        for row in rows:
            print(rows)
        return rows
    except Exception as e:
        logging.error("Error fetching metadata by DOI: %s", str(e))
        raise
    finally:
        conn.close()
def get_metadata_by_doi(doi):
    conn = establish_database_connection()
    cursor = conn.cursor()
    try:
        sql = """
        SELECT
            xxx
        WHERE
            xxx = %s;"""
        cursor.execute(sql, (doi,))
        row = cursor.fetchone()
        return row
    except Exception as e:
        logging.error("Error fetching metadata by DOI: %s", str(e))
        raise
    finally:
        conn.close()
def get_dois_by_acronym(acronym):
    conn = establish_database_connection()
    cursor = conn.cursor()
    try:
        # Updated SQL query with parameterized input for the acronym
        sql = """
        SELECT xxx = %s  
        ORDER BY g.xxx DESC;
        """
        cursor.execute(sql, (acronym,))
        rows = cursor.fetchall()

        # Check if results are empty
        if not rows:
            logging.info("No DOIs found for the acronym: %s", acronym)
            return []

        # Structure the result as a list of dictionaries
        return [
            {
                "doi": row[0],
                "submission_id": row[1],
                "galley_id": row[2],
                "submission_file_id": row[3],
                "journal_path": row[4],
            }
            for row in rows
        ]
    except Exception as e:
        logging.error("Error fetching DOIs for acronym '%s': %s", acronym, str(e))
        raise
    finally:
        conn.close()



def read_file_with_encoding(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    except UnicodeDecodeError:
        with open(file_path, "r", encoding="latin1") as f:
            return f.read()

def download_pdf(pdf_number):
    pdf_url = f"https://eudl.eu/pdf/{pdf_number}"
    directory_name = pdf_number.replace("/", "_").replace(".", "_")
    file_name = f"{directory_name}.pdf"
    full_directory_path = os.path.join(os.getcwd(), directory_name)
    os.makedirs(full_directory_path, exist_ok=True)
    full_file_path = os.path.join(full_directory_path, file_name)

    try:
        response = requests.get(pdf_url, stream=True)
        response.raise_for_status()
        with open(full_file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        return full_directory_path, file_name
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading PDF: {e}")
        return None, None

def get_json(directory, file_name):
    json_file_name = file_name.replace(".pdf", ".json")
    pdf_path = os.path.join(directory, file_name)
    json_path = os.path.join(directory, json_file_name)

    if os.path.exists(json_path):
        return json_path

    curl_command = [
        "curl", "-X", "POST",
        "-F", f"file=@{pdf_path}",
        "172.31.253.18:5080",
        "-F", "types=text title section_header list_item caption formula picture section_Header table page_Footer",
        "-o", json_path
    ]

    try:
        result = subprocess.run(
            curl_command,
            check=True,
            text=True,
            capture_output=True
        )

        if os.path.exists(json_path):
            return json_path
        else:
            return None
    except subprocess.CalledProcessError as e:
        return None

def extract_pub_history_from_json(json_file):
    try:
        content = read_file_with_encoding(json_file)
        if not content.strip():
            raise ValueError(f"The JSON file {json_file} is empty.")
        data = json.loads(content)

        received_date = None
        accepted_date = None
        published_date = None

        for entry in data:
            text = entry.get("text", "")

            received_match = re.search(r"received on (\d{1,2} \w+ \d{4})", text, re.IGNORECASE)
            if received_match:
                received_date = date_parser.parse(received_match.group(1))

            accepted_match = re.search(r"accepted on (\d{1,2} \w+ \d{4})", text, re.IGNORECASE)
            if accepted_match:
                accepted_date = date_parser.parse(accepted_match.group(1))

            published_match = re.search(r"published on (\d{1,2} \w+ \d{4})", text, re.IGNORECASE)
            if published_match:
                published_date = date_parser.parse(published_match.group(1))

            if received_date and accepted_date and published_date:
                break

        if received_date and accepted_date and published_date:
            pub_history = ET.Element("pub-history")

            for event_type, event_date in zip(
                    ["received", "accepted", "published"],
                    [received_date, accepted_date, published_date]
            ):
                event = ET.SubElement(pub_history, "event", {"event-type": event_type})
                date_elem = ET.SubElement(event, "date")
                ET.SubElement(date_elem, "day").text = f"{event_date.day:02}"
                ET.SubElement(date_elem, "month").text = f"{event_date.month:02}"
                ET.SubElement(date_elem, "year").text = str(event_date.year)

            return pub_history

        logging.error("Publication history not found in the JSON.")
        return None

    except Exception as e:
        logging.error(f"Error extracting publication history: {e}")
        return None


def extract_image_from_pdf(pdf_directory, output_directory, image_info):
    """
    Extract an image from the given PDF using the provided image_info dictionary.
    Saves the image with a consistent naming convention and returns the saved path.
    """
    try:
        pdf_file = next((file for file in os.listdir(pdf_directory) if file.endswith(".pdf")), None)
        if not pdf_file:
            raise FileNotFoundError("No PDF file found in the directory.")

        pdf_path = os.path.join(pdf_directory, pdf_file)
        doc = fitz.open(pdf_path)

        # Load the relevant page
        page = doc.load_page(image_info["page_number"] - 1)
        rect = fitz.Rect(
            image_info["left"], image_info["top"],
            image_info["left"] + image_info["width"], image_info["top"] + image_info["height"]
        )
        pix = page.get_pixmap(clip=rect, dpi=300)

        os.makedirs(output_directory, exist_ok=True)

        # Define consistent naming for images
        base_name = "image"
        counter = 1
        while os.path.exists(os.path.join(output_directory, f"{base_name}{counter}.png")):
            counter += 1

        image_path = os.path.join(output_directory, f"{base_name}{counter}.png")
        Image.open(io.BytesIO(pix.tobytes())).save(image_path)

        logging.info(f"Image extracted and saved to {image_path}")
        return image_path

    except Exception as e:
        logging.error(f"Error extracting image: {e}")
        return None


def create_body_from_json(json_file_path, pdf_directory):
    """
    Create an XML <body> element from JSON input and extract images for captions that follow pictures.
    Generates structured XML with label, caption, and graphic for each figure.
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            json_data = json.load(file)

        body = ET.Element("body")
        output_directory = os.path.join(pdf_directory, "media")
        os.makedirs(output_directory, exist_ok=True)

        for i, item in enumerate(json_data):
            item_type = item.get("type", "").lower()
            text = item.get("text", "").strip()

            if "header" in item_type:
                continue  # Skip headers

            # Add text or captions that do not follow the "Figure" format directly to <p>
            if item_type in ["text", "list item", "page footer", "caption"] and not text.startswith("Figure"):
                p = ET.SubElement(body, "p")
                p.text = text

            # Handle captions starting with "Figure" that follow pictures
            elif item_type == "caption" and text.startswith("Figure"):
                if i > 0 and json_data[i - 1].get("type", "").lower() == "picture":
                    prev_image_info = json_data[i - 1]

                    # Extract the image
                    image_path = extract_image_from_pdf(pdf_directory, output_directory, prev_image_info)
                    if image_path:
                        # Add <fig> element with label, caption, and graphic
                        fig = ET.SubElement(body, "fig", id=f"fig{i}", position="float")

                        # Add label: Extract "Figure 1" part
                        label = ET.SubElement(fig, "label")
                        label.text = text.split(".", 1)[0].strip()  # "Figure 1"

                        # Add caption with paragraph
                        caption = ET.SubElement(fig, "caption")
                        p = ET.SubElement(caption, "p")
                        p.text = text.split(".", 1)[1].strip()  # Description part

                        # Add graphic with proper attributes
                        graphic = ET.SubElement(fig, "graphic", attrib={
                            "mimetype": "image",
                            "mime-subtype": "png",
                            "xmlns:xlink": "http://www.w3.org/1999/xlink",
                            "xlink:href": f"media/{os.path.basename(image_path)}"
                        })

        return body

    except Exception as e:
        logging.error(f"Error creating body from JSON: {e}")
        return None


def generate_contrib_group(doi):
    rows = get_metadata_by_con(doi)

    authors = []
    for row in rows:
        author = {
            "given_name": row[0],
            "family_name": row[1],
            "affiliation": row[2]
        }
        authors.append(author)

    contrib_group = ET.Element("contrib-group")

    letters = list(string.ascii_lowercase[:11])

    for index, author in enumerate(authors):
        contrib = ET.SubElement(contrib_group, "contrib", attrib={"contrib-type": "author"})
        name = ET.SubElement(contrib, "name")
        ET.SubElement(name, "surname").text = author['family_name']
        ET.SubElement(name, "given-names").text = author['given_name']

        xref = ET.SubElement(contrib, "xref", attrib={"ref-type": "aff", "rid": f"aff-{letters[index % len(letters)]}"})
        ET.SubElement(xref, "sup").text = letters[index % len(letters)]

    for idx, author in enumerate(authors):
        aff = ET.SubElement(contrib_group, "aff", attrib={"id": f"aff-{letters[idx % len(letters)]}"})
        aff.text = author['affiliation']

    return contrib_group


def read_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: The file at {file_path} is not a valid JSON file.")
        return None

def extract_copyright_line(data):
    for item in data:
        text = item.get('text', '')
        if 'Copyright' in text:
            # Simplify the copyright statement
            return text.strip().split(', licensed to')[0]
    return "Copyright information not found."


def extract_license_description(data):
    for item in data:
        text = item.get('text', '')
        if 'open access article' in text:
            # Simplify the license description
            return text.strip().split(', which permits')[0]
    return "License description not found."


def generate_permissions_xml(data):
    copyright_line = extract_copyright_line(data)
    license_description = extract_license_description(data)

    permissions = ET.Element("permissions")

    # Add copyright-statement
    copyright_statement = ET.SubElement(permissions, "copyright-statement")
    copyright_statement.text = copyright_line

    # Add license
    license_elem = ET.SubElement(permissions, "license", {
        "{http://www.w3.org/1999/xlink}href": "http://creativecommons.org/licenses/by/3.0/"
    })

    # Add license-p with uri
    license_p = ET.SubElement(license_elem, "license-p")
    license_p.text = f"{license_description}, which permits unlimited use, distribution and reproduction in any medium so long as the original work is properly cited."

    # Add uri inside license-p
    uri = ET.SubElement(license_p, "uri", {
        "{http://www.w3.org/1999/xlink}href": "http://creativecommons.org/licenses/by/3.0/"
    })
    uri.text = "http://creativecommons.org/licenses/by/3.0/"

    # Convert to string with pretty formatting
    xml_str = ET.tostring(permissions, encoding="unicode", method="xml")
    return xml_str

def save_dois_to_csv(dois, output_file='data/id_possition/doi_list.csv'):
    try:
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)

            # Write the header row
            writer.writerow(["DOI", "Submission ID", "Galley ID", "Submission File ID", "Journal Path"])

            # Write each DOI record to the CSV file
            for doi in dois:
                writer.writerow([
                    doi["doi"],
                    doi["submission_id"],
                    doi["galley_id"],
                    doi["submission_file_id"],
                    doi["journal_path"],
                ])
        logging.info("DOIs saved successfully to %s", output_file)
    except Exception as e:
        logging.error("Error writing DOIs to CSV: %s", str(e))
        raise


def create_pub_date_xml(publication_date):
    try:
        # Check if publication_date is a date object
        if isinstance(publication_date, (datetime, date)):
            year = publication_date.year
            month = f"{publication_date.month:02d}"
            day = f"{publication_date.day:02d}"
        else:
            # Parse the publication date string into year, month, and day
            pub_date = datetime.strptime(publication_date, '%Y-%m-%d')
            year = pub_date.year
            month = f"{pub_date.month:02d}"
            day = f"{pub_date.day:02d}"

        # Create the XML structure
        # Subscription year
        sub_year = ET.Element("pub-date", {"pub-type": "subscription-year"})
        ET.SubElement(sub_year, "year").text = str(year)

        # Print publication date
        ppub = ET.Element("pub-date", {"pub-type": "ppub"})
        ET.SubElement(ppub, "day").text = day
        ET.SubElement(ppub, "month").text = month
        ET.SubElement(ppub, "year").text = str(year)

        # Electronic publication date
        epub = ET.Element("pub-date", {"pub-type": "epub"})
        ET.SubElement(epub, "day").text = day
        ET.SubElement(epub, "month").text = month
        ET.SubElement(epub, "year").text = str(year)

        # Return the combined elements (not a string)
        root = ET.Element("pub-date-group")
        root.extend([sub_year, ppub, epub])

        return root  # Return the root element containing all the pub-date elements

    except Exception as e:
        logging.error("Error creating XML for publication date: %s", str(e))
        raise

def update_journal_meta_with_article_and_body(doi, output_file_name, pub_history_xml, json_file, pdf_directory):
    if not doi:
        logging.error("No DOI provided. Aborting update.")
        return

    metadata = get_metadata_by_doi(doi)
    if not metadata:
        logging.error("No metadata found for DOI: %s", doi)
        return
    with open("data/id_possition/currentdoi.txt", "a") as doi_file:
        doi_file.write(f"{doi}, {metadata[1]}\n")  # Appends DOI with a newline

    root = ET.Element("root")
    front = ET.SubElement(root, "front")
    journal_meta = ET.SubElement(front, "journal-meta")

    journal_id = ET.SubElement(journal_meta, "journal-id")
    journal_id.set("journal-id-type", "publisher-id")
    journal_id.text = "eai"

    journal_title_group = ET.SubElement(journal_meta, "journal-title-group")
    journal_title = ET.SubElement(journal_title_group, "journal-title")
    journal_title.text = metadata[3]

    issn = ET.SubElement(journal_meta, "issn")
    issn.set("pub-type", "epub")
    issn.text = metadata[9]

    publisher = ET.SubElement(journal_meta, "publisher")
    publisher_name = ET.SubElement(publisher, "publisher-name")
    publisher_name.text = "European Alliance for Innovation"

    article_meta = ET.SubElement(front, "article-meta")
    article_id = ET.SubElement(article_meta, "article-id")
    article_id.set("pub-id-type", "doi")
    article_id.text = metadata[4]  # pub_id_doi

    title_group = ET.SubElement(article_meta, "title-group")
    article_title_element = ET.SubElement(title_group, "article-title")
    article_title_element.text = metadata[5]  # publication_title

    abstract_element = ET.SubElement(article_meta, "abstract")
    abstract_element.text = metadata[6]  # abstract

    contrib_group = generate_contrib_group(doi)
    article_meta.append(contrib_group)

    if pub_history_xml:
        article_meta.append(pub_history_xml)

    publication_date = metadata[7]
    xml_output = create_pub_date_xml(publication_date)
    article_meta.append(xml_output)

    json_data = read_json_file(json_file)
    permissions_xml = generate_permissions_xml(json_data)
    permissions_element = ET.fromstring(permissions_xml)
    article_meta.append(permissions_element)

    body = create_body_from_json(json_file, pdf_directory)
    if body is not None:
        root.append(body)
    else:
        logging.error("Body creation failed; skipping body appending.")

    output_file_path = os.path.join(pdf_directory, output_file_name)
    with open(output_file_path, "wb") as f:
        f.write(ET.tostring(root, encoding="utf-8", xml_declaration=True))
    print(f"Output saved to {output_file_path}")

def process_doi(doi, output_file_name):
    try:
        directory, file_name = download_pdf(doi)
        if not directory or not file_name:
            raise ValueError("Failed to download PDF.")

        json_file = get_json(directory, file_name)
        if not json_file:
            raise ValueError("Failed to create JSON.")

        pub_history_xml = extract_pub_history_from_json(json_file)
        update_journal_meta_with_article_and_body(doi, output_file_name, pub_history_xml, json_file, directory)

    except Exception as e:
        logging.error(f"Error processing DOI {doi}: {e}")
        print(f"Error processing DOI {doi}: {e}")


def main():
    output_file_name = "output.xml"

    try:
        # Ask user for input
        data = input("Enter 'acronym' to fetch DOIs or 'direct' to process the DOI list directly: ").strip()

        if data == "acronym":
            acronym = input("Enter the acronym: ").strip()  # Example: 'iot'
            logging.info("Fetching DOIs for acronym: %s", acronym)
            dois_with_submissions = get_dois_by_acronym(acronym)  # Returns [{'doi': ..., 'submission_id': ...}, ...]
            if dois_with_submissions:
                logging.info("Found %d DOIs for acronym '%s'", len(dois_with_submissions), acronym)
                save_dois_to_csv(dois_with_submissions)
            else:
                logging.info("No DOIs found for acronym '%s'", acronym)
                print("No DOIs found for the provided acronym.")

        elif data == 'direct':
            # Read the last processed DOIs from currentdoi.txt
            processed_dois = set()
            if os.path.exists("data/id_possition/currentdoi.txt"):
                with open("data/id_possition/currentdoi.txt", mode="r", encoding="utf-8") as current_doi_file:
                    processed_dois = set(
                        line.strip() for line in current_doi_file if line.strip()
                    )  # Collect all processed DOIs

            # Read the list of DOIs from doi_list.csv
            with open("data/id_possition/doi_list.csv", mode="r", newline="", encoding="utf-8") as file:
                reader = csv.reader(file)
                # Skip the header row and collect all valid DOIs
                do_list = [row[0].strip() for i, row in enumerate(reader) if i > 0 and row]

            # Remove processed DOIs from do_list
            do_list = [doi for doi in do_list if doi not in processed_dois]

            if not do_list:
                print("No DOIs left to process.")
                return

            print(f"Processing {len(do_list)} DOIs...")

            # Process remaining DOIs using ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [executor.submit(process_doi, doi, output_file_name) for doi in do_list]

                for future, doi in zip(concurrent.futures.as_completed(futures), do_list):
                    # Save the current DOI after it is processed
                    with open("data/id_possition/currentdoi.txt", mode="a", encoding="utf-8") as current_doi_file:
                        current_doi_file.write(f"{doi}\n")

            print("DOI processing completed.")

        else:
            logging.info("Invalid input. Please enter either 'acronym' or 'direct'.")
            print("Invalid input. Please enter either 'acronym' or 'direct'.")

    except FileNotFoundError:
        logging.error("DOI list file 'doi_list.csv' not found.")
        print("DOI list file 'doi_list.csv' not found.")
    except Exception as e:
        logging.error(f"Error processing DOI list: {e}")
        print(f"Error processing DOI list: {e}")


if __name__ == "__main__":
    main()
