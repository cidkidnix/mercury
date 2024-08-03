package REPL

import (
  "bufio"
  "fmt"
  "os"
  "os/exec"
  "strings"
  "mercury/src/LedgerClient"
  "mercury/src/Database"
  "encoding/json"
  "time"
  "strconv"
  "text/template"
  "bytes"
)

type globalReplContext struct {
  ledgerContext *LedgerClient.LedgerContext
  variableMap map[string]interface{}
}

type replContext struct {
  command string
  arguments []string
}

var cliName string = "Mercury"

func printPrompt() {
  fmt.Print(cliName, "> ")
}

func printUnknown(text string) {
  fmt.Println(text, ": command not found")
}

func displayHelp(commands map[string]string) {
  fmt.Printf("These are the available commands: \n")
  for k, v := range(commands) {
    fmt.Println(fmt.Sprintf("(%s) - %s", k, v))
  }
}

func clearScreen() {
  cmd := exec.Command("clear")
  cmd.Stdout = os.Stdout
  cmd.Run()
}

func handleInvalidCmd(text string) {
  fmt.Println(fmt.Sprintf("Invalid Cmd (%s)", text))
}

func cleanInput(text string) string {
    output := strings.TrimSpace(text)
    return output
}

func trimQuotes(text string) string {
  return strings.Trim(text, `"`)
}

func (globalReplContext *globalReplContext) startMercury(context replContext) {
  if globalReplContext.ledgerContext != nil {
    return
  }
  fmt.Println("Mercury Starting")
  connectionVal := new(string)
  configPath := "./config.json"
  startPoint := new(string)
  token := new(string)
  sandbox := false
  applicationId := "daml-script"

  for i := 0; i < len(context.arguments); i++ {
    switch context.arguments[i] {
      case "-connection":
        if len(context.arguments) > i+1 {
            v := trimQuotes(context.arguments[i+1])
            connectionVal = &v
            i++
        }
      case "-start-point":
        if len(context.arguments) > i+1 {
          v := trimQuotes(context.arguments[i+1])
          startPoint = &v
          i++
        }
      case "-config":
        if len(context.arguments) > i+1 {
          configPath = trimQuotes(context.arguments[i+1])
          i++
        }
      case "-sandbox":
        sandbox = true
      case "-application-id":
        if len(context.arguments) > i+1 {
          applicationId = trimQuotes(context.arguments[i+1])
          i++
        }
      case "-token":
        if len(context.arguments) > i+1 {
          v := trimQuotes(context.arguments[i+1])
          token = &v
          i++
        }
    }
  }
  //fmt.Println(fmt.Sprintf("%s", *connectionVal))
  go func()() {
    ledgerClient := LedgerClient.IntializeGRPCConnection(*connectionVal, token, &sandbox, &applicationId, startPoint, configPath)
    globalReplContext.ledgerContext = &ledgerClient
    ledgerClient.WatchTransactionTreeStream()
  }()
  fmt.Println("Mercury started")
}

func (globalReplContext *globalReplContext) listCreates(context replContext) {
  type output struct {
    ContractId string
    TemplateId string
  }

  jsonMode := false
  showTemplateIds := false
  writeToFile := new(string)
  setJson := new(string)
  for i := 0; i < len(context.arguments); i++ {
    switch context.arguments[i] {
      case "-json":
        jsonMode = true
      case "-show-template-ids":
        showTemplateIds = true
      case "-write-to-file":
        if len(context.arguments) > i+1 {
          v := trimQuotes(context.arguments[i+1])
          writeToFile = &v
          i++
        }
      case "-set-json":
        if len(context.arguments) > i+1 {
          v := trimQuotes(context.arguments[i+1])
          setJson = &v
          i++
        }
    }
  }
  if globalReplContext.ledgerContext == nil {
    fmt.Println("Nil")
    return
  }
  db := globalReplContext.ledgerContext.GetDatabaseConnection()
  var contracts []Database.CreatesTable
  db.Find(&contracts)
  var jsonBlob []output
  for i := 0; i < len(contracts); i++ {
    if !jsonMode {
      fmt.Println(fmt.Sprintf("Contract %s", contracts[i].ContractID))
      if showTemplateIds {
        fmt.Println(fmt.Sprintf("TemplateId %s", contracts[i].TemplateFqn))
      }
      if jsonMode {}
    } else {
      contract_id := contracts[i].ContractID
      template_id := ""
      if showTemplateIds {
        template_id = contracts[i].TemplateFqn
      }
      jsonBlob = append(jsonBlob, output{ ContractId: contract_id, TemplateId: template_id  })
    }
  }
  if len(jsonBlob) > 0 {
    f, err := json.Marshal(jsonBlob)
    if err != nil {
      fmt.Println(fmt.Sprintf("Failed to marshal, %s", err))
    }
    if writeToFile != nil {
      os.WriteFile(*writeToFile, f, 0644)
    }
    if setJson != nil {
      m := globalReplContext.variableMap
      m[*setJson] = jsonBlob
      globalReplContext.variableMap = m
    } else {
      fmt.Printf("%s\n", f)
    }
  }
}

func (globalReplContext *globalReplContext) getCommands() (map[string]func(replContext)()) {
  helpMap := make(map[string]string)
  helpMap[".help"] = "Display current page"
  helpMap[".clear"] = "Clear screen"
  helpMap[".start"] = "Start mercury"
  helpMap[".sleep"] = "Sleep for amount of time"
  helpMap["list-creates"] = "List All Creates, Returns ContractIDs"
  helpMap["get"] = "Get variable from global variable map"
  helpMap["set"] = "Set variable in global variable map"
  helpMap["load"] = "Load File of commands"
  helpMap["echo"] = "Echo string verbatim"
  helpMap["template"] = "Template string, can provide -execute to run the command"
  helpMap[".time"] = "Time exeuction of commands"
  helpMap["->"] = "Chain together commands"

  commandsMap := make(map[string]func(context replContext)())
  commandsMap[".help"] = func(context replContext)(){
      displayHelp(helpMap)
  }
  commandsMap[".clear"] = func(context replContext)(){
      clearScreen()
  }
  commandsMap[".start"] = func(context replContext)(){
      globalReplContext.startMercury(context)
  }
  commandsMap["list-creates"] = func(context replContext)(){
      globalReplContext.listCreates(context)
  }
  commandsMap[".sleep"] = func(context replContext)(){
    decode, err := strconv.Atoi(context.arguments[0])
    if err != nil {
      fmt.Println(fmt.Sprintf("Failed to decode %s", err))
    }
    time.Sleep(time.Duration(decode) * time.Second)
  }
  commandsMap[".time"] = func(context replContext)(){
    joined := strings.Join(context.arguments, " ")
    now := time.Now()
    globalReplContext.chainedCommand(joined)
    end := time.Since(now)
    fmt.Println(fmt.Sprintf("Command took %v", end))
  }
  commandsMap["set"] = func(context replContext)(){
    m := globalReplContext.variableMap
    m[context.arguments[0]] = strings.Join(context.arguments[1:], " ")
    globalReplContext.variableMap = m
  }
  commandsMap["unset"] = func(context replContext)(){
    m := globalReplContext.variableMap
    delete(m, context.arguments[0])
    globalReplContext.variableMap = m
  }
  commandsMap["get"] = func(context replContext)(){
    if len(globalReplContext.variableMap) > 0 {
      fmt.Println(fmt.Sprintf("%s", globalReplContext.variableMap[context.arguments[0]]))
    }
  }
  commandsMap["template"] = func(context replContext)(){
    var args string
    if context.arguments[0] == "-execute" {
      args = strings.Join(context.arguments[1:], " ")
    } else {
      args = strings.Join(context.arguments, " ")
    }
    tmpl, err := template.New("f").Parse(args)
    if err != nil {
      fmt.Println(fmt.Sprintf("Failed to template %s", err))
    }
    var t bytes.Buffer
    err = tmpl.Execute(&t, globalReplContext.variableMap)
    if err != nil {
      fmt.Println(fmt.Sprintf("Failed to template %s", err))
    }
    str := t.String()
    if context.arguments[0] == "-execute" {
        globalReplContext.chainedCommand(str)
    } else {
      fmt.Println(fmt.Sprintf("%s", str))
    }
  }
  commandsMap["echo"] = func(context replContext)(){
    args := strings.Join(context.arguments, " ")
    fmt.Println(fmt.Sprintf("%s", args))
  }
  commandsMap["load"] = func(context replContext)(){
    b, err := os.ReadFile(context.arguments[0])
    if err != nil {
      fmt.Print(err)
    }

    str := string(b)
    str = strings.TrimSuffix(str, "\n")
    newString := strings.Replace(str, "\n", " -> ", -1)
    globalReplContext.chainedCommand(newString)
  }
  commandsMap["#"] = func(context replContext)(){
    if _, ok := globalReplContext.variableMap["comments_are_prints"]; ok {
      commandsMap["echo"](context)
    }
    // stub
  }
  commandsMap["get-ledger-id"] = func(context replContext)(){
    if globalReplContext.ledgerContext == nil {
      return
    }
    ledger_id := globalReplContext.ledgerContext.GetLedgerId()
    m := globalReplContext.variableMap
    m["ledger_id"] = ledger_id
    globalReplContext.variableMap = m
  }

  return commandsMap
}

func (globalReplContext *globalReplContext) chainedCommand(commandString string) {
  cmdToExec := strings.Split(commandString, " ")

  var commandStops []int
  var nextCommands []string
  commands := globalReplContext.getCommands()
  for i := 0; i < len(cmdToExec); i++ {
    switch cmdToExec[i] {
      case "->":
        commandStops = append(commandStops, i+1)
    }
  }
  // .time is special, as it takes one argument and measure the full round trip
  if len(commandStops) == 0 || cmdToExec[0] == ".time" {
    if val, ok := commands[cmdToExec[0]]; ok {
      val(replContext{ command: cmdToExec[0], arguments: cmdToExec[1:] })
    }
    return
  }
  nextCommands = append(nextCommands, strings.Join(cmdToExec[0:(commandStops[0] - 1)], " "))
  for i := 0; i < len(commandStops); i++ {
    if len(commandStops) > i+1 {
        nextCommands = append(nextCommands, strings.Join(cmdToExec[commandStops[i]:(commandStops[i+1] - 1)], " "))
    } else {
        nextCommands = append(nextCommands, strings.Join(cmdToExec[commandStops[i]:], " "))
    }
  }
  for _, i := range(nextCommands) {
    nextCommand := strings.Split(i, " ")
    if val, ok := commands[nextCommand[0]]; ok {
      if _, oo := globalReplContext.variableMap["print_command"]; oo {
        fmt.Println(fmt.Sprintf("Running Command: %s", strings.Join(nextCommand, " ")))
      }
      val(replContext{ command: nextCommand[0], arguments: nextCommand[1:] })
    }
  }
}

func Repl() {
  variableM := make(map[string]interface{})
  globalCtx := globalReplContext{
    ledgerContext: nil,
    variableMap: variableM,
  }
  reader := bufio.NewScanner(os.Stdin)
  printPrompt()
  for reader.Scan() {
    text := cleanInput(reader.Text())
    globalCtx.chainedCommand(text)
    printPrompt()
  }
  fmt.Println()
}
