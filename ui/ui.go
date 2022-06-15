package ui

import (
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	textStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Render
	spinnerStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF00F2"))
)

type Model struct {
	LineCount           int
	SuccessCount        int
	UnprocessedCount    int
	UnprocessedFileName string
	Running             bool
	RemainCount         *int64
	index               int
	spinner             spinner.Model
	Percent             float64
	FinCh               chan struct{}
}

type Option struct {
	LineCount   int
	RemainCount *int64
	FinCh       chan struct{}
}

type BatchMsg struct {
	SuccessCount        int
	UnprocessedCount    int
	UnprocessedFileName string
}

type tickMsg time.Time

func InitModel(opt *Option) Model {
	m := Model{
		LineCount:   opt.LineCount,
		Running:     false,
		RemainCount: opt.RemainCount,
		FinCh:       opt.FinCh,
	}
	m.resetSpinner()

	return m
}

func (m Model) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}
	case BatchMsg:
		m.SuccessCount += msg.SuccessCount
		m.UnprocessedCount += msg.UnprocessedCount
		m.Percent = float64(m.SuccessCount) / float64(m.LineCount)

		if m.SuccessCount+m.UnprocessedCount == m.LineCount {
			return m, func() tea.Msg {
				m.FinCh <- struct{}{}
				return tea.Quit
			}
		}

		return m, nil
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}

	return m, nil
}

func (m *Model) resetSpinner() {
	m.spinner = spinner.New()
	m.spinner.Style = spinnerStyle
	m.spinner.Spinner = spinner.Dot
}

func (m Model) View() string {
	var s string
	if m.UnprocessedFileName == "" {
		s = fmt.Sprintf("%sSuccess: %d(%d%%)", m.spinner.View(), m.SuccessCount, int(m.Percent*100))
	} else if m.SuccessCount+m.UnprocessedCount == m.LineCount {
		s = fmt.Sprintln("All done!")
	} else {
		s = fmt.Sprintf("%sSuccess: %d(%d%%) Unprocessed(%s): %d",
			m.spinner.View(),
			m.SuccessCount,
			int(m.Percent*100),
			m.UnprocessedFileName,
			m.UnprocessedCount)
	}

	return s
}
