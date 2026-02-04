package core

import (
	"log"

	"codeberg.org/go-pdf/fpdf"
)

func createPDF(text string) error {
	pdf := fpdf.New("P", "mm", "A4", "")
	pdf.AddPage()
	pdf.SetFont("Arial", "B", 16)
	pdf.Cell(40, 10, text)
	err := pdf.OutputFileAndClose("hello.pdf")
	if err != nil {
		log.Printf("Error message: %s", err)
		return err
	}

	return nil
}
